//! Process management for external agent runs and Claude sessions
//!
//! This module provides process lifecycle management:
//! - Registration and tracking of running processes
//! - Graceful and forceful termination
//! - Live output capture
//! - Cross-platform process killing
//!
//! # Overview
//!
//! The process registry maintains a centralized record of all running agent processes
//! and Claude sessions. It provides thread-safe access to process information and
//! handles cross-platform process termination.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │       ProcessRegistry (Central)         │
//! │  ┌───────────────────────────────────┐  │
//! │  │   HashMap<run_id, ProcessHandle>  │  │
//! │  │  - Process metadata               │  │
//! │  │  - Child process handle           │  │
//! │  │  - Live output buffer             │  │
//! │  └───────────────────────────────────┘  │
//! └─────────────────────────────────────────┘
//!            ▲                    ▲
//!            │                    │
//!     ┌──────┴─────┐      ┌──────┴─────┐
//!     │ Agent Run  │      │  Claude    │
//!     │   Process  │      │  Session   │
//!     └────────────┘      └────────────┘
//! ```
//!
//! # Usage Example
//!
//! ```rust,no_run
//! use bamboo_agent::process::registry::{ProcessRegistry, ProcessRegistrationConfig};
//! use std::sync::{Arc, Mutex};
//! use tokio::process::Command;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), String> {
//!     // Create registry
//!     let registry = ProcessRegistry::new();
//!
//!     // Spawn a process
//!     let mut child = Command::new("my-agent")
//!         .arg("--task")
//!         .arg("analyze")
//!         .spawn()
//!         .map_err(|e| e.to_string())?;
//!
//!     let pid = child.id().unwrap_or(0);
//!
//!     // Register the process
//!     let config = ProcessRegistrationConfig {
//!         run_id: 1000001,
//!         agent_id: 1,
//!         agent_name: "CodeAnalyzer".to_string(),
//!         pid,
//!         project_path: "/project".to_string(),
//!         task: "analyze code".to_string(),
//!         model: "claude-3-5-sonnet".to_string(),
//!     };
//!
//!     registry.register_process(config, child).await?;
//!
//!     // Later, kill the process
//!     registry.kill_process(1000001).await?;
//!
//!     Ok(())
//! }
//! ```

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::process::Child;
use tokio::sync::Mutex as AsyncMutex;

/// Type of process being tracked in the registry
///
/// This enum distinguishes between different types of processes that
/// can be managed by the registry, each with their own metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProcessType {
    /// An agent run process spawned by the server
    AgentRun {
        /// Unique identifier for the agent
        agent_id: i64,
        /// Human-readable name of the agent
        agent_name: String,
    },

    /// A Claude interactive session process
    ClaudeSession {
        /// Session identifier for the Claude conversation
        session_id: String,
    },
}

/// Metadata about a registered process
///
/// Contains all the information needed to track, monitor, and manage
/// a running process throughout its lifecycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessInfo {
    /// Unique run identifier for this process execution
    pub run_id: i64,

    /// Type of process (agent run or Claude session)
    pub process_type: ProcessType,

    /// Operating system process ID
    pub pid: u32,

    /// Timestamp when the process was started
    pub started_at: DateTime<Utc>,

    /// Project directory where the process is running
    pub project_path: String,

    /// Task description or prompt being executed
    pub task: String,

    /// Model identifier being used (e.g., "claude-3-5-sonnet")
    pub model: String,
}

/// Configuration for registering a new agent process
///
/// This struct contains all the parameters needed to register
/// an agent run process in the registry.
#[derive(Debug, Clone)]
pub struct ProcessRegistrationConfig {
    /// Unique run identifier
    pub run_id: i64,

    /// Agent identifier
    pub agent_id: i64,

    /// Human-readable agent name
    pub agent_name: String,

    /// Operating system process ID
    pub pid: u32,

    /// Project directory path
    pub project_path: String,

    /// Task description
    pub task: String,

    /// Model being used
    pub model: String,
}

/// Internal handle to a registered process
///
/// Combines process metadata with runtime resources needed to
/// manage the process lifecycle and capture output.
#[allow(dead_code)]
pub struct ProcessHandle {
    /// Process metadata and configuration
    pub info: ProcessInfo,

    /// Handle to the child process (if available)
    ///
    /// This is wrapped in Arc<Mutex> to allow shared ownership
    /// and thread-safe access for process management operations.
    pub child: Arc<Mutex<Option<Child>>>,

    /// Buffer for capturing live process output
    ///
    /// Stores stdout/stderr output as it's generated, allowing
    /// clients to retrieve recent output at any time.
    pub live_output: Arc<Mutex<String>>,
}

/// Central registry for managing running agent processes
///
/// The ProcessRegistry maintains a thread-safe map of all running processes,
/// providing lifecycle management, monitoring, and termination capabilities.
///
/// # Thread Safety
///
/// The registry uses async-aware locking (`tokio::sync::Mutex`) for the
/// process map to avoid blocking async tasks. Process handles use
/// standard `std::sync::Mutex` for synchronous operations.
///
/// # Process Lifecycle
///
/// ```text
/// 1. Register → Process added to registry with metadata
/// 2. Running  → Process executes, output is captured
/// 3. Kill     → Graceful shutdown attempted, then force kill
/// 4. Cleanup  → Process removed from registry
/// ```
pub struct ProcessRegistry {
    /// Map of run IDs to process handles
    processes: Arc<AsyncMutex<HashMap<i64, ProcessHandle>>>,

    /// Counter for generating unique run IDs
    ///
    /// Starts at 1,000,000 to distinguish from lower numbered IDs
    /// that might be used elsewhere in the system.
    next_id: Arc<Mutex<i64>>,
}

impl ProcessRegistry {
    /// Create a new empty process registry
    ///
    /// Initializes the registry with an empty process map and
    /// sets the ID counter to start at 1,000,000.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use bamboo_agent::process::registry::ProcessRegistry;
    ///
    /// let registry = ProcessRegistry::new();
    /// ```
    pub fn new() -> Self {
        Self {
            processes: Arc::new(AsyncMutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(1000000)),
        }
    }

    /// Generate a unique run ID
    ///
    /// Returns the next available run ID and increments the counter.
    /// IDs start at 1,000,000 and increase sequentially.
    ///
    /// # Errors
    ///
    /// Returns an error if the ID counter mutex is poisoned.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use bamboo_agent::process::registry::ProcessRegistry;
    ///
    /// let registry = ProcessRegistry::new();
    /// let id = registry.generate_id().unwrap();
    /// assert!(id >= 1000000);
    /// ```
    pub fn generate_id(&self) -> Result<i64, String> {
        let mut next_id = self.next_id.lock().map_err(|e| e.to_string())?;
        let id = *next_id;
        *next_id += 1;
        Ok(id)
    }

    /// Register a new agent run process
    ///
    /// Adds a newly spawned agent process to the registry with its
    /// configuration and process handle.
    ///
    /// # Arguments
    ///
    /// * `config` - Registration configuration with agent metadata
    /// * `child` - The spawned child process handle
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if registration succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if the process map lock fails.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use bamboo_agent::process::registry::{ProcessRegistry, ProcessRegistrationConfig};
    /// use tokio::process::Command;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), String> {
    ///     let registry = ProcessRegistry::new();
    ///
    ///     let mut child = Command::new("agent-binary")
    ///         .spawn()
    ///         .map_err(|e| e.to_string())?;
    ///
    ///     let config = ProcessRegistrationConfig {
    ///         run_id: 1000001,
    ///         agent_id: 1,
    ///         agent_name: "MyAgent".to_string(),
    ///         pid: child.id().unwrap_or(0),
    ///         project_path: "/project".to_string(),
    ///         task: "Analyze code".to_string(),
    ///         model: "claude-3-5-sonnet".to_string(),
    ///     };
    ///
    ///     registry.register_process(config, child).await
    /// }
    /// ```
    pub async fn register_process(
        &self,
        config: ProcessRegistrationConfig,
        child: Child,
    ) -> Result<(), String> {
        let ProcessRegistrationConfig {
            run_id,
            agent_id,
            agent_name,
            pid,
            project_path,
            task,
            model,
        } = config;

        let process_info = ProcessInfo {
            run_id,
            process_type: ProcessType::AgentRun {
                agent_id,
                agent_name,
            },
            pid,
            started_at: Utc::now(),
            project_path,
            task,
            model,
        };

        self.register_process_internal(run_id, process_info, child)
            .await
    }

    /// Register a sidecar process without a direct child handle
    ///
    /// Used for processes that are managed externally (e.g., by Tauri)
    /// but still need to be tracked in the registry for monitoring.
    ///
    /// # Arguments
    ///
    /// * `config` - Registration configuration with agent metadata
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if registration succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if the process map lock fails.
    pub async fn register_sidecar_process(
        &self,
        config: ProcessRegistrationConfig,
    ) -> Result<(), String> {
        let ProcessRegistrationConfig {
            run_id,
            agent_id,
            agent_name,
            pid,
            project_path,
            task,
            model,
        } = config;

        let process_info = ProcessInfo {
            run_id,
            process_type: ProcessType::AgentRun {
                agent_id,
                agent_name,
            },
            pid,
            started_at: Utc::now(),
            project_path,
            task,
            model,
        };

        let mut processes = self.processes.lock().await;

        let process_handle = ProcessHandle {
            info: process_info,
            child: Arc::new(Mutex::new(None)),
            live_output: Arc::new(Mutex::new(String::new())),
        };

        processes.insert(run_id, process_handle);
        Ok(())
    }

    /// Register a Claude interactive session process
    ///
    /// Adds a Claude session to the registry, generating a unique run ID
    /// if one isn't provided.
    ///
    /// # Arguments
    ///
    /// * `session_id` - Claude session identifier
    /// * `pid` - Operating system process ID
    /// * `project_path` - Project directory path
    /// * `task` - Task description
    /// * `model` - Model identifier
    /// * `child` - Optional child process handle wrapped in Arc<Mutex>
    ///
    /// # Returns
    ///
    /// Returns the generated or provided run ID on success.
    ///
    /// # Errors
    ///
    /// Returns an error if ID generation or process map lock fails.
    pub async fn register_claude_session(
        &self,
        session_id: String,
        pid: u32,
        project_path: String,
        task: String,
        model: String,
        child: Arc<Mutex<Option<Child>>>,
    ) -> Result<i64, String> {
        let run_id = self.generate_id()?;

        let process_info = ProcessInfo {
            run_id,
            process_type: ProcessType::ClaudeSession { session_id },
            pid,
            started_at: Utc::now(),
            project_path,
            task,
            model,
        };

        let mut processes = self.processes.lock().await;

        let process_handle = ProcessHandle {
            info: process_info,
            child,
            live_output: Arc::new(Mutex::new(String::new())),
        };

        processes.insert(run_id, process_handle);
        Ok(run_id)
    }

    /// Internal helper to register a process in the map
    async fn register_process_internal(
        &self,
        run_id: i64,
        process_info: ProcessInfo,
        child: Child,
    ) -> Result<(), String> {
        let mut processes = self.processes.lock().await;

        let process_handle = ProcessHandle {
            info: process_info,
            child: Arc::new(Mutex::new(Some(child))),
            live_output: Arc::new(Mutex::new(String::new())),
        };

        processes.insert(run_id, process_handle);
        Ok(())
    }

    /// Get all running Claude session processes
    ///
    /// Returns a list of all registered Claude sessions that are
    /// currently tracked in the registry.
    ///
    /// # Returns
    ///
    /// Vector of `ProcessInfo` for all Claude sessions.
    ///
    /// # Errors
    ///
    /// Returns an error if the process map lock fails.
    pub async fn get_running_claude_sessions(&self) -> Result<Vec<ProcessInfo>, String> {
        let processes = self.processes.lock().await;
        Ok(processes
            .values()
            .filter_map(|handle| match &handle.info.process_type {
                ProcessType::ClaudeSession { .. } => Some(handle.info.clone()),
                _ => None,
            })
            .collect())
    }

    /// Find a Claude session by its session ID
    ///
    /// Searches the registry for a Claude session matching the
    /// provided session identifier.
    ///
    /// # Arguments
    ///
    /// * `session_id` - The Claude session ID to search for
    ///
    /// # Returns
    ///
    /// `Some(ProcessInfo)` if found, `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if the process map lock fails.
    pub async fn get_claude_session_by_id(
        &self,
        session_id: &str,
    ) -> Result<Option<ProcessInfo>, String> {
        let processes = self.processes.lock().await;
        Ok(processes
            .values()
            .find(|handle| match &handle.info.process_type {
                ProcessType::ClaudeSession { session_id: sid } => sid == session_id,
                _ => false,
            })
            .map(|handle| handle.info.clone()))
    }

    /// Remove a process from the registry
    ///
    /// Unregisters a process by its run ID. This does NOT kill the process;
    /// it only removes it from tracking.
    ///
    /// # Arguments
    ///
    /// * `run_id` - The run ID of the process to remove
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` whether or not the process existed.
    ///
    /// # Errors
    ///
    /// Returns an error if the process map lock fails.
    pub async fn unregister_process(&self, run_id: i64) -> Result<(), String> {
        let mut processes = self.processes.lock().await;
        processes.remove(&run_id);
        Ok(())
    }

    /// Synchronous version for use in non-async contexts
    ///
    /// Uses `try_lock` to avoid blocking. If the lock is held,
    /// the operation is skipped (process will be cleaned up later).
    #[allow(dead_code)]
    fn unregister_process_sync(&self, run_id: i64) -> Result<(), String> {
        // Use try_lock to avoid blocking in sync context
        // If we can't get the lock, that's okay - the process will be cleaned up later
        if let Ok(mut processes) = self.processes.try_lock() {
            processes.remove(&run_id);
        }
        Ok(())
    }

    /// Get all registered processes (agent runs and Claude sessions)
    ///
    /// Returns a list of all processes currently in the registry.
    ///
    /// # Returns
    ///
    /// Vector of `ProcessInfo` for all registered processes.
    ///
    /// # Errors
    ///
    /// Returns an error if the process map lock fails.
    #[allow(dead_code)]
    pub async fn get_running_processes(&self) -> Result<Vec<ProcessInfo>, String> {
        let processes = self.processes.lock().await;
        Ok(processes
            .values()
            .map(|handle| handle.info.clone())
            .collect())
    }

    /// Get all running agent run processes
    ///
    /// Returns a list of all agent run processes (excluding Claude sessions)
    /// currently tracked in the registry.
    ///
    /// # Returns
    ///
    /// Vector of `ProcessInfo` for agent run processes.
    ///
    /// # Errors
    ///
    /// Returns an error if the process map lock fails.
    pub async fn get_running_agent_processes(&self) -> Result<Vec<ProcessInfo>, String> {
        let processes = self.processes.lock().await;
        Ok(processes
            .values()
            .filter_map(|handle| match &handle.info.process_type {
                ProcessType::AgentRun { .. } => Some(handle.info.clone()),
                _ => None,
            })
            .collect())
    }

    /// Get process information by run ID
    ///
    /// Retrieves metadata for a specific process.
    ///
    /// # Arguments
    ///
    /// * `run_id` - The run ID to look up
    ///
    /// # Returns
    ///
    /// `Some(ProcessInfo)` if found, `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if the process map lock fails.
    #[allow(dead_code)]
    pub async fn get_process(&self, run_id: i64) -> Result<Option<ProcessInfo>, String> {
        let processes = self.processes.lock().await;
        Ok(processes.get(&run_id).map(|handle| handle.info.clone()))
    }

    /// Kill a process by run ID
    ///
    /// Attempts graceful shutdown first using the child process handle,
    /// then falls back to system-level process termination if needed.
    ///
    /// # Process
    ///
    /// 1. Send kill signal via child handle
    /// 2. Wait up to 5 seconds for graceful exit
    /// 3. If timeout, use system kill command (kill -KILL or taskkill)
    /// 4. Remove process from registry
    ///
    /// # Arguments
    ///
    /// * `run_id` - The run ID of the process to kill
    ///
    /// # Returns
    ///
    /// `Ok(true)` if the process was killed successfully,
    /// `Ok(false)` if the process wasn't found.
    ///
    /// # Errors
    ///
    /// Returns an error if process termination fails critically.
    ///
    /// # Cross-Platform
    ///
    /// - Unix: Uses SIGTERM first, then SIGKILL if needed
    /// - Windows: Uses taskkill /F
    pub async fn kill_process(&self, run_id: i64) -> Result<bool, String> {
        use log::{error, info, warn};

        let (pid, child_arc) = {
            let processes = self.processes.lock().await;
            if let Some(handle) = processes.get(&run_id) {
                (handle.info.pid, handle.child.clone())
            } else {
                warn!("Process {} not found in registry", run_id);
                return Ok(false);
            }
        };

        info!(
            "Attempting graceful shutdown of process {} (PID: {})",
            run_id, pid
        );

        let kill_sent = {
            let mut child_guard = child_arc.lock().map_err(|e| e.to_string())?;
            if let Some(child) = child_guard.as_mut() {
                match child.start_kill() {
                    Ok(_) => {
                        info!("Successfully sent kill signal to process {}", run_id);
                        true
                    }
                    Err(e) => {
                        error!("Failed to send kill signal to process {}: {}", run_id, e);
                        false
                    }
                }
            } else {
                warn!(
                    "No child handle available for process {} (PID: {}), attempting system kill",
                    run_id, pid
                );
                false
            }
        };

        if !kill_sent {
            info!(
                "Attempting fallback kill for process {} (PID: {})",
                run_id, pid
            );
            match self.kill_process_by_pid(run_id, pid).await {
                Ok(true) => return Ok(true),
                Ok(false) => warn!(
                    "Fallback kill also failed for process {} (PID: {})",
                    run_id, pid
                ),
                Err(e) => error!("Error during fallback kill: {}", e),
            }
        }

        let wait_result = tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
            loop {
                let status = {
                    let mut child_guard = child_arc.lock().map_err(|e| e.to_string())?;
                    if let Some(child) = child_guard.as_mut() {
                        match child.try_wait() {
                            Ok(Some(status)) => {
                                info!("Process {} exited with status: {:?}", run_id, status);
                                *child_guard = None;
                                Some(Ok::<(), String>(()))
                            }
                            Ok(None) => None,
                            Err(e) => {
                                error!("Error checking process status: {}", e);
                                Some(Err(e.to_string()))
                            }
                        }
                    } else {
                        Some(Ok(()))
                    }
                };

                match status {
                    Some(result) => return result,
                    None => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                }
            }
        })
        .await;

        match wait_result {
            Ok(Ok(_)) => {
                info!("Process {} exited gracefully", run_id);
            }
            Ok(Err(e)) => {
                error!("Error waiting for process {}: {}", run_id, e);
            }
            Err(_) => {
                warn!("Process {} didn't exit within 5 seconds after kill", run_id);
                if let Ok(mut child_guard) = child_arc.lock() {
                    *child_guard = None;
                }
                let _ = self.kill_process_by_pid(run_id, pid).await;
            }
        }

        self.unregister_process(run_id).await?;

        Ok(true)
    }

    /// Kill a process by its operating system PID
    ///
    /// Uses system commands to terminate a process when the child
    /// handle is not available or has already been dropped.
    ///
    /// # Arguments
    ///
    /// * `run_id` - Run ID for registry cleanup
    /// * `pid` - Operating system process ID
    ///
    /// # Returns
    ///
    /// `Ok(true)` if kill succeeded, `Ok(false)` if it failed.
    ///
    /// # Errors
    ///
    /// Returns an error if the kill command cannot be executed.
    ///
    /// # Cross-Platform Behavior
    ///
    /// - Unix: Tries SIGTERM first, waits 2 seconds, then SIGKILL
    /// - Windows: Uses taskkill /F immediately
    pub async fn kill_process_by_pid(&self, run_id: i64, pid: u32) -> Result<bool, String> {
        use log::{error, info, warn};

        info!("Attempting to kill process {} by PID {}", run_id, pid);

        let kill_result = if cfg!(target_os = "windows") {
            let pid_str = pid.to_string();
            crate::core::process_utils::trace_windows_command(
                "process_registry.kill_process_by_pid",
                "taskkill",
                ["/F", "/PID", pid_str.as_str()],
            );
            let mut command = std::process::Command::new("taskkill");
            crate::core::process_utils::hide_window_for_std_command(&mut command);
            command.args(["/F", "/PID", &pid_str]).output()
        } else {
            let term_result = std::process::Command::new("kill")
                .args(["-TERM", &pid.to_string()])
                .output();

            match &term_result {
                Ok(output) if output.status.success() => {
                    info!("Sent SIGTERM to PID {}", pid);
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

                    let check_result = std::process::Command::new("kill")
                        .args(["-0", &pid.to_string()])
                        .output();

                    if let Ok(output) = check_result {
                        if output.status.success() {
                            warn!(
                                "Process {} still running after SIGTERM, sending SIGKILL",
                                pid
                            );
                            std::process::Command::new("kill")
                                .args(["-KILL", &pid.to_string()])
                                .output()
                        } else {
                            term_result
                        }
                    } else {
                        term_result
                    }
                }
                _ => {
                    warn!("SIGTERM failed for PID {}, trying SIGKILL", pid);
                    std::process::Command::new("kill")
                        .args(["-KILL", &pid.to_string()])
                        .output()
                }
            }
        };

        match kill_result {
            Ok(output) => {
                if output.status.success() {
                    info!("Successfully killed process with PID {}", pid);
                    self.unregister_process(run_id).await?;
                    Ok(true)
                } else {
                    let error_msg = String::from_utf8_lossy(&output.stderr);
                    warn!("Failed to kill PID {}: {}", pid, error_msg);
                    Ok(false)
                }
            }
            Err(e) => {
                error!("Failed to execute kill command for PID {}: {}", pid, e);
                Err(format!("Failed to execute kill command: {}", e))
            }
        }
    }

    /// Check if a process is still running
    ///
    /// Uses `try_wait()` to check if the process has exited without blocking.
    ///
    /// # Arguments
    ///
    /// * `run_id` - The run ID to check
    ///
    /// # Returns
    ///
    /// `Ok(true)` if the process is still running,
    /// `Ok(false)` if it has exited or doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if lock acquisition fails.
    #[allow(dead_code)]
    pub async fn is_process_running(&self, run_id: i64) -> Result<bool, String> {
        let processes = self.processes.lock().await;

        if let Some(handle) = processes.get(&run_id) {
            let child_arc = handle.child.clone();
            drop(processes);

            let mut child_guard = child_arc.lock().map_err(|e| e.to_string())?;
            if let Some(ref mut child) = child_guard.as_mut() {
                match child.try_wait() {
                    Ok(Some(_)) => {
                        *child_guard = None;
                        Ok(false)
                    }
                    Ok(None) => Ok(true),
                    Err(_) => {
                        *child_guard = None;
                        Ok(false)
                    }
                }
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    /// Append output to a process's live output buffer
    ///
    /// Adds a line of output to the process's output buffer for later retrieval.
    ///
    /// # Arguments
    ///
    /// * `run_id` - The run ID of the process
    /// * `output` - The output text to append
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` whether or not the process exists.
    ///
    /// # Errors
    ///
    /// Returns an error if lock acquisition fails.
    pub async fn append_live_output(&self, run_id: i64, output: &str) -> Result<(), String> {
        let processes = self.processes.lock().await;
        if let Some(handle) = processes.get(&run_id) {
            let mut live_output = handle.live_output.lock().map_err(|e| e.to_string())?;
            live_output.push_str(output);
            live_output.push('\n');
        }
        Ok(())
    }

    /// Retrieve the live output buffer for a process
    ///
    /// Gets all captured output for the specified process.
    ///
    /// # Arguments
    ///
    /// * `run_id` - The run ID of the process
    ///
    /// # Returns
    ///
    /// The accumulated output string, or empty string if process not found.
    ///
    /// # Errors
    ///
    /// Returns an error if lock acquisition fails.
    pub async fn get_live_output(&self, run_id: i64) -> Result<String, String> {
        let processes = self.processes.lock().await;
        if let Some(handle) = processes.get(&run_id) {
            let live_output = handle.live_output.lock().map_err(|e| e.to_string())?;
            Ok(live_output.clone())
        } else {
            Ok(String::new())
        }
    }

    /// Remove all finished processes from the registry
    ///
    /// Checks each registered process and removes those that have exited.
    /// Useful for periodic cleanup.
    ///
    /// # Returns
    ///
    /// Vector of run IDs that were removed.
    ///
    /// # Errors
    ///
    /// Returns an error if process checks or lock acquisition fails.
    #[allow(dead_code)]
    pub async fn cleanup_finished_processes(&self) -> Result<Vec<i64>, String> {
        let mut finished_runs = Vec::new();

        {
            let processes = self.processes.lock().await;
            let run_ids: Vec<i64> = processes.keys().cloned().collect();
            drop(processes);

            for run_id in run_ids {
                if !self.is_process_running(run_id).await? {
                    finished_runs.push(run_id);
                }
            }
        }

        {
            let mut processes = self.processes.lock().await;
            for run_id in &finished_runs {
                processes.remove(run_id);
            }
        }

        Ok(finished_runs)
    }
}

impl Default for ProcessRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_append_and_get_live_output() {
        let registry = ProcessRegistry::new();
        let run_id = registry
            .register_claude_session(
                "session-1".to_string(),
                1234,
                "/tmp/project".to_string(),
                "task".to_string(),
                "model".to_string(),
                Arc::new(Mutex::new(None)),
            )
            .await
            .unwrap();

        registry.append_live_output(run_id, "line1").await.unwrap();
        registry.append_live_output(run_id, "line2").await.unwrap();

        let output = registry.get_live_output(run_id).await.unwrap();
        assert_eq!(output, "line1\nline2\n");
    }
}
