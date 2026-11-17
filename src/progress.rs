use std::{
    fmt::Display,
    io::{self, IsTerminal, Write},
    time::Instant,
};

pub(crate) struct Progress {
    label: &'static str,
    start: Instant,
    terminal: bool,
    width: usize,
}

impl Progress {
    pub(crate) fn new(label: &'static str) -> Self {
        Self {
            label,
            start: Instant::now(),
            terminal: io::stderr().is_terminal(),
            width: 0,
        }
    }

    pub(crate) fn update(&mut self, message: impl Display) {
        self.draw(message, false);
    }

    pub(crate) fn finish(mut self, message: impl Display) {
        self.draw(message, true);
    }

    fn draw(&mut self, message: impl Display, finished: bool) {
        let line = format!(
            "{} {} [{:.1}s]",
            self.label,
            message,
            self.start.elapsed().as_secs_f64()
        );
        let mut stderr = io::stderr().lock();
        if self.terminal {
            let _ = write!(stderr, "\r{line:width$}", width = self.width);
            self.width = if finished { 0 } else { line.len() };
            if finished {
                let _ = writeln!(stderr);
            }
            let _ = stderr.flush();
        } else {
            let _ = writeln!(stderr, "{line}");
        }
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        if self.width > 0 {
            let _ = writeln!(io::stderr().lock());
        }
    }
}

pub(crate) fn bar(done: u64, total: u64) -> String {
    const WIDTH: usize = 16;
    let percent = if total == 0 {
        100
    } else {
        (u128::from(done.min(total)) * 100 / u128::from(total)) as usize
    };
    let filled = percent * WIDTH / 100;
    format!(
        "[{}{}] {percent:>3}%",
        "=".repeat(filled),
        "-".repeat(WIDTH - filled)
    )
}
