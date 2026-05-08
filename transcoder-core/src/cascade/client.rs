use tonic::{transport::{Channel, Endpoint, ClientTlsConfig, Certificate}, Request};
use crate::proto::exa::language_server_pb::{
    language_server_service_client::LanguageServerServiceClient,
    StartCascadeRequest, SendUserCascadeMessageRequest, GetCascadeTrajectoryRequest,
};
use crate::proto::exa::cortex_pb::{CortexTrajectoryType, CascadeRunStatus, CortexStepType, CortexStepStatus};
use crate::proto::exa::codeium_common_pb::{Metadata, TextOrScopeItem};
use crate::proto::exa::reactive_component_pb::{StreamReactiveUpdatesRequest, MessageDiff};
use crate::mappers::CascadeDelta; // 👈 引入新的增量类型
use tokio::time::sleep;
use std::time::{Duration, Instant};
use tokio_stream::StreamExt;

pub struct CascadeClient {
// ... (omitting new() for brevity, it's already there)
// Wait, I should include the whole impl block or use replace_file_content carefully.

// Let's refine the replacement to the chat_stream method.
    client: LanguageServerServiceClient<Channel>,
    metadata: Metadata,
    auth_token: String,
}

#[derive(Debug, Clone, Copy, Default)]
struct PlannerStepCursor {
    text_len: usize,
    thinking_len: usize,
}

fn shorten_for_progress(value: &str, max_chars: usize) -> String {
    let trimmed = value.trim();
    let mut chars = trimmed.chars();
    let shortened: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{}...", shortened)
    } else {
        shortened.to_string()
    }
}

fn step_status_label(status: i32) -> &'static str {
    if status == CortexStepStatus::Generating as i32 {
        "生成中"
    } else if status == CortexStepStatus::Running as i32 {
        "运行中"
    } else if status == CortexStepStatus::Waiting as i32 {
        "等待中"
    } else if status == CortexStepStatus::Done as i32 {
        "已完成"
    } else if status == CortexStepStatus::Error as i32 {
        "出错"
    } else if status == CortexStepStatus::Queued as i32 {
        "排队中"
    } else if status == CortexStepStatus::Pending as i32 {
        "待执行"
    } else {
        "处理中"
    }
}

fn progress_for_step(
    step_index: usize,
    step: &crate::proto::gemini_coder::Step,
) -> Option<(String, String)> {
    let status = step_status_label(step.status);
    let kind = step.step.as_ref()?;

    match kind {
        crate::proto::gemini_coder::step::Step::SearchWeb(search) => {
            let query = shorten_for_progress(&search.query, 80);
            let query_label = if query.is_empty() {
                "搜索网络".to_string()
            } else {
                format!("搜索网络：{}", query)
            };
            let count = search.web_documents.len();
            let signature = format!("{}:{}:{}:{}", step_index, step.status, search.query, count);
            let message = if count > 0 {
                format!("{}（{}，已返回 {} 条结果）", query_label, status, count)
            } else {
                format!("{}（{}）", query_label, status)
            };
            Some((signature, message))
        }
        crate::proto::gemini_coder::step::Step::ReadUrlContent(read) => {
            let url = if read.resolved_url.is_empty() {
                &read.url
            } else {
                &read.resolved_url
            };
            if url.is_empty() {
                return None;
            }
            let short_url = shorten_for_progress(url, 100);
            Some((
                format!(
                    "{}:{}:{}:{}",
                    step_index, step.status, read.url, read.resolved_url
                ),
                format!("读取网页内容：{}（{}）", short_url, status),
            ))
        }
        crate::proto::gemini_coder::step::Step::ViewContentChunk(chunk) => {
            if chunk.document_id.is_empty() {
                return None;
            }
            let id = shorten_for_progress(&chunk.document_id, 80);
            Some((
                format!("{}:{}:{}", step_index, step.status, chunk.document_id),
                format!("查看检索内容片段：{}（{}）", id, status),
            ))
        }
        crate::proto::gemini_coder::step::Step::KnowledgeGeneration(_) => Some((
            format!("{}:{}", step_index, step.status),
            format!("整理检索材料（{}）", status),
        )),
        crate::proto::gemini_coder::step::Step::WriteToFile(write) => {
            if write.target_file_uri.is_empty() {
                return None;
            }
            let target = shorten_for_progress(&write.target_file_uri, 100);
            Some((
                format!("{}:{}:{}", step_index, step.status, write.target_file_uri),
                format!("写入文件：{}（{}）", target, status),
            ))
        }
        crate::proto::gemini_coder::step::Step::McpTool(tool) => {
            let tool_name = tool
                .tool_call
                .as_ref()
                .map(|call| call.name.as_str())
                .filter(|name| !name.is_empty())
                .unwrap_or("MCP 工具");
            let progress = if tool.progress_message.is_empty() {
                tool_name.to_string()
            } else {
                format!("{}：{}", tool_name, shorten_for_progress(&tool.progress_message, 80))
            };
            Some((
                format!(
                    "{}:{}:{}:{}:{}",
                    step_index, step.status, tool.server_name, tool_name, tool.progress_message
                ),
                format!("执行 {}（{}）", progress, status),
            ))
        }
        crate::proto::gemini_coder::step::Step::RunCommand(command) => {
            let cmd = if command.command_line.is_empty() {
                &command.command
            } else {
                &command.command_line
            };
            if cmd.is_empty() {
                return None;
            }
            Some((
                format!("{}:{}:{}:{:?}", step_index, step.status, cmd, command.exit_code),
                format!("执行命令：{}（{}）", shorten_for_progress(cmd, 100), status),
            ))
        }
        crate::proto::gemini_coder::step::Step::NotifyUser(notification) => {
            if notification.notification_content.is_empty() {
                return None;
            }
            let message = shorten_for_progress(&notification.notification_content, 100);
            Some((
                format!("{}:{}:{}", step_index, step.status, notification.notification_content),
                format!("通知：{}（{}）", message, status),
            ))
        }
        _ => None,
    }
}

fn save_image_to_disk(image_data: &[u8], mime_type: &str) -> String {
    let mime = if mime_type.is_empty() { "image/png" } else { mime_type };
    let file_id = uuid::Uuid::new_v4();
    let ext = if mime.contains("jpeg") || mime.contains("jpg") { "jpg" } else { "png" };
    let ws_dir = crate::common::get_app_data_dir().join("images");
    let _ = std::fs::create_dir_all(&ws_dir);
    let disk_img_path = ws_dir.join(format!("img_{}.{}", file_id, ext));
    let disk_b64_path = ws_dir.join(format!("img_{}.b64.txt", file_id));
    
    let _ = std::fs::write(&disk_img_path, image_data);
    
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD.encode(image_data);
    let data_url = format!("data:{};base64,{}", mime, b64);
    let _ = std::fs::write(&disk_b64_path, &data_url);

    format!(
        "\n\n![Generated Image]({})\n\n*(图片亦保存至本地: `{}`，Base64格式备份于: `{}`)*\n\n",
        disk_img_path.display(), disk_img_path.display(), disk_b64_path.display()
    )
}

/// 从 Reactive Diff 中递归提取文本增量 (针对 PlannerResponse.response)
/// Trajectory (1: steps) -> Step (20: planner_response) -> PlannerResponse (1: response)
fn extract_text_from_diff(diff: &MessageDiff, path: &[u32]) -> Option<String> {
    if path.is_empty() { return None; }
    let target_field = path[0];
    
    for fd in &diff.field_diffs {
        if fd.field_number == target_field {
            use crate::proto::exa::reactive_component_pb::field_diff::Diff;
            match &fd.diff {
                Some(Diff::UpdateSingular(sv)) => {
                    use crate::proto::exa::reactive_component_pb::singular_value::Value;
                    match &sv.value {
                        Some(Value::StringValue(s)) if path.len() == 1 => return Some(s.clone()),
                        Some(Value::MessageValue(inner_diff)) if path.len() > 1 => {
                            return extract_text_from_diff(inner_diff, &path[1..]);
                        }
                        _ => {}
                    }
                }
                Some(Diff::UpdateRepeated(rd)) => {
                    // 对于 steps (field 1)，通常在最后追加
                    if target_field == 1 && path.len() > 1 {
                        for val in rd.update_values.iter().rev() {
                            use crate::proto::exa::reactive_component_pb::singular_value::Value;
                            if let Some(Value::MessageValue(inner_diff)) = &val.value {
                                if let Some(txt) = extract_text_from_diff(inner_diff, &path[1..]) {
                                    return Some(txt);
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
    None
}

impl CascadeClient {
    pub async fn new(
        grpc_addr: String,
        metadata: Metadata,
        csrf_token: String,
        tls_cert: Vec<u8>,
        workspace_dir: Option<String>,
    ) -> anyhow::Result<Self> {
        let addr = if grpc_addr.starts_with("http") { grpc_addr } else { format!("https://{}", grpc_addr) };
        
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&tls_cert))
            .domain_name("localhost");

        let channel = Endpoint::from_shared(addr)?
            .tls_config(tls)?
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_while_idle(true)
            .connect_timeout(Duration::from_secs(10))
            .connect()
            .await?;
            
        let mut client = LanguageServerServiceClient::new(channel);

        // 🚀 如果有工作区目录，向 LS 引擎注册工作区
        // 注意两个 gRPC 方法对路径格式的要求不同：
        //   AddTrackedWorkspace 要求裸绝对路径（如 /Users/xxx/cse3）
        //   SetWorkingDirectories 要求 file:// URI 格式（如 file:///Users/xxx/cse3）
        if let Some(ref dir) = workspace_dir {
            if !dir.is_empty() {
                let bare_path = dir.trim_start_matches("file://").to_string();
                let uri = if bare_path.starts_with('/') {
                    format!("file://{}", bare_path)
                } else {
                    format!("file:///{}", bare_path)
                };

                // AddTrackedWorkspace: 裸绝对路径
                let add_ws_req = crate::proto::exa::language_server_pb::AddTrackedWorkspaceRequest {
                    workspace: bare_path.clone(),
                    do_not_watch_files: true,
                    is_passive_workspace: false,
                };
                let mut grpc_req = Request::new(add_ws_req);
                grpc_req.metadata_mut().insert("x-codeium-csrf-token", csrf_token.parse().unwrap());
                if let Err(e) = client.add_tracked_workspace(grpc_req).await {
                    tracing::warn!("⚠️ [Cascade] 注册工作区失败 ({}): {:?}", bare_path, e);
                } else {
                    tracing::info!("✅ [Cascade] 工作区已注册: {}", bare_path);
                }

                // SetWorkingDirectories: file:// URI 格式
                let set_wd_req = crate::proto::exa::language_server_pb::SetWorkingDirectoriesRequest {
                    directory_uris: vec![uri.clone()],
                };
                let mut grpc_req2 = Request::new(set_wd_req);
                grpc_req2.metadata_mut().insert("x-codeium-csrf-token", csrf_token.parse().unwrap());
                if let Err(e) = client.set_working_directories(grpc_req2).await {
                    tracing::warn!("⚠️ [Cascade] 设置工作目录失败 ({}): {:?}", uri, e);
                } else {
                    tracing::info!("✅ [Cascade] 工作目录已设置: {}", uri);
                }
            }
        }

        Ok(Self {
            client,
            metadata,
            auth_token: csrf_token,
        })
    }

    fn auth_request<T>(&self, msg: T) -> Request<T> {
        let mut req = Request::new(msg);
        req.metadata_mut().insert("x-codeium-csrf-token", self.auth_token.parse().unwrap());
        req
    }

    /// 发起一次 Cascade 对话并开启流式增量输出
    pub async fn chat_stream(
        &mut self, 
        user_text: String, 
        model_id: i32, 
        images: Vec<crate::proto::exa::codeium_common_pb::ImageData>,
        media: Vec<crate::proto::exa::codeium_common_pb::Media>,
        force_reasoning_before_text: bool,
    ) -> Result<tokio::sync::mpsc::Receiver<Result<CascadeDelta, tonic::Status>>, anyhow::Error> {
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        let model_enum = model_id;
        let mut client_clone = self.client.clone();
        let csrf_token_clone = self.auth_token.clone();

        // 构造统一的 CascadeConfig
        let cascade_config = Some(crate::proto::exa::cortex_pb::CascadeConfig {
            planner_config: Some(crate::proto::exa::cortex_pb::CascadePlannerConfig {
                requested_model: Some(crate::proto::exa::codeium_common_pb::ModelOrAlias {
                    choice: Some(crate::proto::exa::codeium_common_pb::model_or_alias::Choice::Model(model_enum)),
                }),
                planner_type_config: Some(crate::proto::exa::cortex_pb::cascade_planner_config::PlannerTypeConfig::Conversational(
                    crate::proto::exa::cortex_pb::CascadeConversationalPlannerConfig::default()
                )),
                ..Default::default()
            }),
            ..Default::default()
        });

        // 1. StartCascade
        let start_req = StartCascadeRequest {
            metadata: Some(self.metadata.clone()),
            trajectory_type: CortexTrajectoryType::Cascade as i32,
            ..Default::default()
        };
        let start_resp = self.client.start_cascade(self.auth_request(start_req)).await.map_err(|status| {
            tracing::error!("❌ [Cascade] StartCascade failed. Status: {:?}, Metadata: {:?}", status, status.metadata());
            anyhow::anyhow!("StartCascade 错误 [{}]: {}", status.code(), status.message())
        })?.into_inner();
        let cascade_id = start_resp.cascade_id;
        tracing::info!("🚀 [Cascade] 已启动会话 (流流结合): {}", cascade_id);

        if !images.is_empty() {
            tracing::info!("📸 [Cascade] 当前请求包含 {} 张图片(Base64) 和 {} 个媒体对象", images.len(), media.len());
        }

        // 2. SendUserCascadeMessage
        let send_req = SendUserCascadeMessageRequest {
            metadata: Some(self.metadata.clone()),
            cascade_id: cascade_id.clone(),
            items: vec![TextOrScopeItem {
                chunk: Some(crate::proto::exa::codeium_common_pb::text_or_scope_item::Chunk::Text(user_text)),
            }],
            images,
            media,
            cascade_config,
            ..Default::default()
        };
        self.client.send_user_cascade_message(self.auth_request(send_req)).await.map_err(|status| {
            tracing::error!("❌ [Cascade] SendUserCascadeMessage failed. Status: {:?}, Metadata: {:?}", status, status.metadata());
            anyhow::anyhow!("SendUserCascadeMessage 错误 [{}]: {}", status.code(), status.message())
        })?;

        // 3. 轮询 Fallback 与 最终快照确认
        tokio::spawn(async move {
            let mut planner_step_cursors: Vec<PlannerStepCursor> = Vec::new();
            let mut error_retry_count = 0;
            let mut idle_poll_count = 0;
            let mut last_progress_at = Instant::now();
            let mut last_visible_progress_at = Instant::now();
            let mut pending_text_deltas: Vec<String> = Vec::new();
            let mut progress_step_signatures: Vec<String> = Vec::new();
            let mut ended_with_error = false;

            loop {
                let mut req = Request::new(GetCascadeTrajectoryRequest {
                    cascade_id: cascade_id.clone(),
                    ..Default::default()
                });
                req.metadata_mut().insert("x-codeium-csrf-token", csrf_token_clone.parse().unwrap());

                match client_clone.get_cascade_trajectory(req).await {
                    Ok(resp) => {
                        error_retry_count = 0;
                        let mut made_progress = false;
                        let traj_resp = resp.into_inner();
                        if let Some(traj) = &traj_resp.trajectory {
                            let mut snapshot_thinking_deltas = Vec::new();
                            let mut snapshot_progress_deltas = Vec::new();
                            let mut snapshot_text_deltas = Vec::new();

                            // 逐个处理所有 PlannerResponse step。一次对话中可能会出现多个独立的
                            // thinking phase；如果只盯着“最新一个 step + 全局长度游标”，在 step
                            // 切换后新的 thinking 通常会因为长度重置而被跳过，表现为流卡住。
                            for (step_index, step) in traj.steps.iter().enumerate() {
                                if force_reasoning_before_text {
                                    if let Some((signature, message)) = progress_for_step(step_index, step) {
                                        if progress_step_signatures.len() <= step_index {
                                            progress_step_signatures.resize(step_index + 1, String::new());
                                        }
                                        if progress_step_signatures[step_index] != signature {
                                            progress_step_signatures[step_index] = signature;
                                            snapshot_progress_deltas.push(format!("\n[进度] {}\n", message));
                                            made_progress = true;
                                        }
                                    }
                                }

                                if step.r#type != CortexStepType::PlannerResponse as i32 {
                                    continue;
                                }

                                let Some(crate::proto::gemini_coder::step::Step::PlannerResponse(pr)) = &step.step else {
                                    continue;
                                };

                                if planner_step_cursors.len() <= step_index {
                                    planner_step_cursors.resize(step_index + 1, PlannerStepCursor::default());
                                }

                                let cursor = &mut planner_step_cursors[step_index];

                                // 某些独立 phase 会切到新的 PlannerResponse step；极端情况下同一 step
                                // 也可能被上游重写，导致字符串长度回退。这里显式重置该 step 的游标。
                                if pr.response.len() < cursor.text_len {
                                    tracing::warn!(
                                        "⚠️ [Cascade] PlannerResponse step {} response length regressed: {} -> {}",
                                        step_index,
                                        cursor.text_len,
                                        pr.response.len()
                                    );
                                    cursor.text_len = 0;
                                }
                                if pr.thinking.len() < cursor.thinking_len {
                                    tracing::warn!(
                                        "⚠️ [Cascade] PlannerResponse step {} thinking length regressed: {} -> {}",
                                        step_index,
                                        cursor.thinking_len,
                                        pr.thinking.len()
                                    );
                                    cursor.thinking_len = 0;
                                }

                                // 处理思考链 Thinking (字段 3)
                                if pr.thinking.len() > cursor.thinking_len {
                                    let delta = &pr.thinking[cursor.thinking_len..];
                                    snapshot_thinking_deltas.push(delta.to_string());
                                    cursor.thinking_len = pr.thinking.len();
                                    made_progress = true;
                                }
                                // 处理正文 Text
                                if pr.response.len() > cursor.text_len {
                                    let delta = &pr.response[cursor.text_len..];
                                    snapshot_text_deltas.push(delta.to_string());
                                    cursor.text_len = pr.response.len();
                                    made_progress = true;
                                }
                            }

                            if force_reasoning_before_text
                                && snapshot_thinking_deltas.is_empty()
                                && snapshot_progress_deltas.is_empty()
                                && snapshot_text_deltas.is_empty()
                                && traj_resp.status != CascadeRunStatus::Idle as i32
                                && last_visible_progress_at.elapsed() > Duration::from_secs(20)
                            {
                                snapshot_progress_deltas.push(
                                    "\n[进度] Antigravity 内核仍在执行，等待新的模型或工具输出。\n".to_string(),
                                );
                            }

                            let visible_now = !snapshot_thinking_deltas.is_empty()
                                || !snapshot_progress_deltas.is_empty()
                                || (!force_reasoning_before_text && !snapshot_text_deltas.is_empty());

                            // 同一轮询快照可能包含多个 PlannerResponse step。始终先发完本轮所有
                            // thinking/progress；thinking 模型的 text 额外延迟到轮询结束后再释放。
                            for delta in snapshot_thinking_deltas {
                                if tx.send(Ok(CascadeDelta::Thinking(delta))).await.is_err() {
                                    break;
                                }
                            }
                            for delta in snapshot_progress_deltas {
                                if tx.send(Ok(CascadeDelta::Thinking(delta))).await.is_err() {
                                    break;
                                }
                            }
                            if force_reasoning_before_text {
                                pending_text_deltas.extend(snapshot_text_deltas);
                            } else {
                                pending_text_deltas.extend(snapshot_text_deltas);
                                for delta in pending_text_deltas.drain(..) {
                                    if tx.send(Ok(CascadeDelta::Text(delta))).await.is_err() {
                                        break;
                                    }
                                }
                            }

                            if visible_now {
                                last_visible_progress_at = Instant::now();
                            }

                            if made_progress {
                                last_progress_at = Instant::now();
                                idle_poll_count = 0;
                            }

                            // 状态退出逻辑
                            if traj_resp.status == CascadeRunStatus::Idle as i32 {
                                idle_poll_count += 1;
                            } else {
                                idle_poll_count = 0;
                            }

                            if idle_poll_count > 10 {
                                break;
                            }
                        }

                        // 只在长时间没有任何 Text/Thinking 增量时退出。Thinking 模型持续输出
                        // reasoning_content 时会不断刷新 last_progress_at，不应被固定总时长切断。
                        if last_progress_at.elapsed() > Duration::from_secs(300) {
                            tracing::warn!("⚠️ [Cascade] 5 分钟无任何增量，结束轮询");
                            break;
                        }
                    }
                    Err(s) => {
                        if error_retry_count < 5 {
                            sleep(Duration::from_secs(1 << error_retry_count)).await;
                            error_retry_count += 1;
                        } else {
                            let _ = tx.send(Err(s)).await;
                            ended_with_error = true;
                            break;
                        }
                    }
                }

                sleep(Duration::from_millis(500)).await;
            }

            if !ended_with_error {
                for delta in pending_text_deltas.drain(..) {
                    if tx.send(Ok(CascadeDelta::Text(delta))).await.is_err() {
                        break;
                    }
                }
            }
        });

        Ok(rx)
    }
}
