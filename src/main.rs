use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{SqlitePool, sqlite::SqlitePoolOptions};
use std::net::SocketAddr;
use tower_http::cors::{Any, CorsLayer};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
struct LearningProgress {
    id: Uuid,
    user_id: String,
    course_id: String,
    chapter_id: String,
    section_id: String,
    progress_percent: f32,
    last_position: Option<f32>,
    completed: bool,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct SaveProgressRequest {
    user_id: String,
    course_id: String,
    chapter_id: String,
    section_id: String,
    progress_percent: f32,
    last_position: Option<f32>,
    completed: Option<bool>,
}

#[derive(Debug, Serialize)]
struct ProgressResponse {
    id: Uuid,
    user_id: String,
    course_id: String,
    chapter_id: String,
    section_id: String,
    progress_percent: f32,
    last_position: Option<f32>,
    completed: bool,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct LastLearningNode {
    chapter_id: String,
    section_id: String,
    progress_percent: f32,
    last_position: Option<f32>,
    completed: bool,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct ResumePlayback {
    chapter_id: String,
    section_id: String,
    seek_position: f32,
    progress_percent: f32,
    completed: bool,
    is_resume: bool,
    last_studied_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct ApiResponse<T> {
    code: i32,
    message: String,
    data: Option<T>,
}

struct AppState {
    pool: SqlitePool,
}

async fn init_db() -> Result<SqlitePool, Box<dyn std::error::Error>> {
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect("sqlite:progress.db?mode=rwc")
        .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS learning_progress (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            course_id TEXT NOT NULL,
            chapter_id TEXT NOT NULL,
            section_id TEXT NOT NULL,
            progress_percent REAL NOT NULL DEFAULT 0,
            last_position REAL,
            completed INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(user_id, course_id, chapter_id, section_id)
        )
        "#,
    )
    .execute(&pool)
    .await?;

    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_user_course 
        ON learning_progress(user_id, course_id, updated_at DESC)
        "#,
    )
    .execute(&pool)
    .await?;

    Ok(pool)
}

async fn save_progress(
    State(state): State<AppState>,
    Json(req): Json<SaveProgressRequest>,
) -> impl IntoResponse {
    let now = Utc::now();
    let id = Uuid::new_v4();
    let completed = req.completed.unwrap_or(req.progress_percent >= 100.0);

    let result = sqlx::query_as::<_, LearningProgress>(
        r#"
        INSERT INTO learning_progress 
            (id, user_id, course_id, chapter_id, section_id, 
             progress_percent, last_position, completed, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id, course_id, chapter_id, section_id) DO UPDATE SET
            progress_percent = MAX(learning_progress.progress_percent, excluded.progress_percent),
            last_position = CASE
                WHEN excluded.last_position IS NOT NULL
                THEN MAX(COALESCE(learning_progress.last_position, 0), excluded.last_position)
                ELSE learning_progress.last_position
            END,
            completed = MAX(learning_progress.completed, excluded.completed),
            updated_at = CASE
                WHEN excluded.progress_percent > learning_progress.progress_percent
                     OR (excluded.last_position IS NOT NULL
                          AND excluded.last_position > COALESCE(learning_progress.last_position, 0))
                THEN excluded.updated_at
                ELSE learning_progress.updated_at
            END
        RETURNING *
        "#,
    )
    .bind(id.to_string())
    .bind(&req.user_id)
    .bind(&req.course_id)
    .bind(&req.chapter_id)
    .bind(&req.section_id)
    .bind(req.progress_percent)
    .bind(req.last_position)
    .bind(completed)
    .bind(now.to_rfc3339())
    .bind(now.to_rfc3339())
    .fetch_one(&state.pool)
    .await;

    match result {
        Ok(progress) => Json(ApiResponse {
            code: 0,
            message: "保存成功".to_string(),
            data: Some(ProgressResponse {
                id: progress.id,
                user_id: progress.user_id,
                course_id: progress.course_id,
                chapter_id: progress.chapter_id,
                section_id: progress.section_id,
                progress_percent: progress.progress_percent,
                last_position: progress.last_position,
                completed: progress.completed,
                updated_at: progress.updated_at,
            }),
        })
        .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<()> {
                code: 500,
                message: format!("保存失败: {}", e),
                data: None,
            }),
        )
            .into_response(),
    }
}

async fn get_last_learning_node(
    State(state): State<AppState>,
    Path((user_id, course_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let result = sqlx::query_as::<_, LearningProgress>(
        r#"
        SELECT * FROM learning_progress
        WHERE user_id = ? AND course_id = ?
        ORDER BY updated_at DESC
        LIMIT 1
        "#,
    )
    .bind(&user_id)
    .bind(&course_id)
    .fetch_optional(&state.pool)
    .await;

    match result {
        Ok(Some(progress)) => Json(ApiResponse {
            code: 0,
            message: "获取成功".to_string(),
            data: Some(LastLearningNode {
                chapter_id: progress.chapter_id,
                section_id: progress.section_id,
                progress_percent: progress.progress_percent,
                last_position: progress.last_position,
                completed: progress.completed,
                updated_at: progress.updated_at,
            }),
        })
        .into_response(),
        Ok(None) => Json(ApiResponse {
            code: 404,
            message: "未找到学习记录".to_string(),
            data: None::<LastLearningNode>,
        })
        .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<()> {
                code: 500,
                message: format!("获取失败: {}", e),
                data: None,
            }),
        )
            .into_response(),
    }
}

async fn get_resume_playback(
    State(state): State<AppState>,
    Path((user_id, course_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let result = sqlx::query_as::<_, LearningProgress>(
        r#"
        SELECT * FROM learning_progress
        WHERE user_id = ? AND course_id = ?
        ORDER BY updated_at DESC
        LIMIT 1
        "#,
    )
    .bind(&user_id)
    .bind(&course_id)
    .fetch_optional(&state.pool)
    .await;

    match result {
        Ok(Some(progress)) => {
            let seek_position = progress.last_position.unwrap_or(0.0);
            let is_resume = seek_position > 0.0 || progress.progress_percent > 0.0;

            Json(ApiResponse {
                code: 0,
                message: if is_resume {
                    "续播节点已就绪".to_string()
                } else {
                    "首次学习，从头开始".to_string()
                },
                data: Some(ResumePlayback {
                    chapter_id: progress.chapter_id,
                    section_id: progress.section_id,
                    seek_position,
                    progress_percent: progress.progress_percent,
                    completed: progress.completed,
                    is_resume,
                    last_studied_at: progress.updated_at,
                }),
            })
            .into_response()
        }
        Ok(None) => Json(ApiResponse {
            code: 0,
            message: "无学习记录，从头开始".to_string(),
            data: None::<ResumePlayback>,
        })
        .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<()> {
                code: 500,
                message: format!("获取失败: {}", e),
                data: None,
            }),
        )
            .into_response(),
    }
}

async fn get_course_progress(
    State(state): State<AppState>,
    Path((user_id, course_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let result = sqlx::query_as::<_, LearningProgress>(
        r#"
        SELECT * FROM learning_progress
        WHERE user_id = ? AND course_id = ?
        ORDER BY updated_at DESC
        "#,
    )
    .bind(&user_id)
    .bind(&course_id)
    .fetch_all(&state.pool)
    .await;

    match result {
        Ok(progress_list) => {
            let response: Vec<ProgressResponse> = progress_list
                .into_iter()
                .map(|p| ProgressResponse {
                    id: p.id,
                    user_id: p.user_id,
                    course_id: p.course_id,
                    chapter_id: p.chapter_id,
                    section_id: p.section_id,
                    progress_percent: p.progress_percent,
                    last_position: p.last_position,
                    completed: p.completed,
                    updated_at: p.updated_at,
                })
                .collect();

            Json(ApiResponse {
                code: 0,
                message: "获取成功".to_string(),
                data: Some(response),
            })
            .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<()> {
                code: 500,
                message: format!("获取失败: {}", e),
                data: None,
            }),
        )
            .into_response(),
    }
}

async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "timestamp": Utc::now().to_rfc3339()
    }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = init_db().await?;
    println!("数据库初始化成功");

    let state = AppState { pool };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/api/progress", post(save_progress))
        .route(
            "/api/progress/last/:user_id/:course_id",
            get(get_last_learning_node),
        )
        .route(
            "/api/progress/resume/:user_id/:course_id",
            get(get_resume_playback),
        )
        .route(
            "/api/progress/course/:user_id/:course_id",
            get(get_course_progress),
        )
        .layer(cors)
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    println!("服务器运行在 http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
