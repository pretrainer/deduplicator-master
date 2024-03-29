use console::{style, Style};
use similar::{ChangeTag, TextDiff};
use std::fmt;

struct Line(Option<usize>);

impl fmt::Display for Line {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0 {
            None => write!(f, "    "),
            Some(idx) => write!(f, "{:<4}", idx + 1),
        }
    }
}

pub fn print_diff(old: &str, new: &str) {
    let diff = TextDiff::from_lines(old, new);

    println!("{:-^1$}", "-", 80);
    for op in diff.ops() {
        for change in diff.iter_inline_changes(op) {
            let (sign, s) = match change.tag() {
                ChangeTag::Delete => ("-", Style::new().red()),
                ChangeTag::Insert => ("+", Style::new().green()),
                ChangeTag::Equal => (" ", Style::new().dim()),
            };
            print!(
                "{}{} |{}",
                style(Line(change.old_index())).dim(),
                style(Line(change.new_index())).dim(),
                s.apply_to(sign).bold(),
            );
            for (emphasized, value) in change.iter_strings_lossy() {
                if emphasized {
                    print!("{}", s.apply_to(value).underlined().on_black());
                } else {
                    print!("{}", s.apply_to(value));
                }
            }
            if change.missing_newline() {
                println!();
            }
        }
    }
}
