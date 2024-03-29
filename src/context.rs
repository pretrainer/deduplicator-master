use anyhow::Result;
use cityhasher::hash;
use std::{
    collections::HashMap,
    fs::create_dir_all,
    path::{Component, Path, PathBuf},
};

#[derive(Clone)]
pub struct Context {
    input_folder: String,
    tmp: String,
    input_files: Vec<String>,
    hash_to_input_file: HashMap<u16, Vec<usize>>,
}

impl Context {
    pub fn new(input_folder: String, pattern: String, tmp: String) -> Result<Self> {
        let walker =
            globwalk::GlobWalkerBuilder::from_patterns(&input_folder, &[pattern]).build()?;
        let input_files: Vec<String> = walker
            .into_iter()
            .filter_map(Result::ok)
            .map(|x| {
                std::fs::canonicalize(x.path())
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string()
            })
            .collect();

        let mut hash_to_input_file: HashMap<u16, Vec<usize>> = HashMap::new();
        for (i, path) in (&input_files).iter().enumerate() {
            hash_to_input_file
                .entry(Self::hash_path(&path))
                .or_default()
                .push(i);
        }

        create_dir_all(format!("{}/filters", tmp))?;

        Ok(Self {
            input_folder: Self::canonicalize(&input_folder),
            tmp: Self::canonicalize(&tmp),
            input_files,
            hash_to_input_file,
        })
    }

    pub fn hash_path(path: &String) -> u16 {
        let x = hash::<u32>(path);
        ((x >> 16) ^ x) as u16
    }

    pub fn canonicalize(path: &String) -> String {
        let path = Path::new(path);
        let mut components = path.components().peekable();
        let mut ret = if let Some(c @ Component::Prefix(..)) = components.peek().cloned() {
            components.next();
            PathBuf::from(c.as_os_str())
        } else {
            PathBuf::new()
        };

        for component in components {
            match component {
                Component::Prefix(..) => unreachable!(),
                Component::RootDir => {
                    ret.push(component.as_os_str());
                }
                Component::CurDir => {}
                Component::ParentDir => {
                    ret.pop();
                }
                Component::Normal(c) => {
                    ret.push(c);
                }
            }
        }
        ret.display().to_string()
    }

    pub fn input_files(&self) -> &Vec<String> {
        &self.input_files
    }

    pub fn hash_to_input_files(&self, hash: u16) -> Vec<String> {
        if !self.hash_to_input_file.contains_key(&hash) {
            return Vec::new();
        }

        let mut result = Vec::new();
        for index in self.hash_to_input_file.get(&hash).unwrap() {
            result.push(self.input_files[*index].clone());
        }
        result
    }

    pub fn raw_lsh_buckets_folder_path(&self) -> String {
        let path = format!("{}/raw_lsh_buckets", self.tmp);
        Self::canonicalize(&path)
    }

    pub fn input_folder(&self) -> &String {
        &self.input_folder
    }

    pub fn duplicats_groups_path(&self) -> String {
        let path = format!("{}/duplicates.groups", self.tmp);
        Self::canonicalize(&path)
    }

    pub fn filter_file_path(&self, path_hash: u16) -> String {
        let path = format!("{}/filters/{}.filter", self.tmp, path_hash);
        Self::canonicalize(&path)
    }
}
