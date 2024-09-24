#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum Uri {
    LocalFileSystem(std::path::PathBuf),
    S3 { bucket: String, key: String },
    Gcs { bucket: String, key: String },
    Raw(String),
}

impl std::fmt::Display for Uri {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Uri::LocalFileSystem(path) => write!(f, "{}", path.to_string_lossy()),
            Uri::S3 { bucket, key } => write!(f, "s3://{}/{}", bucket, key),
            Uri::Gcs { bucket, key } => write!(f, "gs://{}/{}", bucket, key),
            Uri::Raw(raw) => write!(f, "{}", raw),
        }
    }
}
