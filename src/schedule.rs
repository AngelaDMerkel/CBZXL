use anyhow::{Result, anyhow, ensure};
use chrono::{Datelike, Duration, LocalResult, NaiveDate, TimeZone, Utc, Weekday};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "cadence", rename_all = "snake_case")]
pub enum Cadence {
    Daily {
        hour: u32,
        minute: u32,
    },
    Weekly {
        weekday: u32,
        hour: u32,
        minute: u32,
    },
    Monthly {
        #[serde(alias = "day_of_month")]
        day: i32,
        hour: u32,
        minute: u32,
    },
    Interval {
        #[serde(alias = "interval_value")]
        value: i64,
        #[serde(alias = "interval_unit")]
        unit: IntervalUnit,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IntervalUnit {
    Hours,
    Days,
}

impl Cadence {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Daily { .. } => "daily",
            Self::Weekly { .. } => "weekly",
            Self::Monthly { .. } => "monthly",
            Self::Interval { .. } => "interval",
        }
    }

    pub fn validate(&self) -> Result<()> {
        let valid_time = match *self {
            Self::Daily { hour, minute }
            | Self::Weekly { hour, minute, .. }
            | Self::Monthly { hour, minute, .. } => hour < 24 && minute < 60,
            Self::Interval { value, .. } => value > 0 && value <= 8760,
        };
        ensure!(valid_time, "invalid schedule cadence");
        ensure!(
            !matches!(self, Self::Weekly { weekday, .. } if *weekday > 6),
            "invalid weekday"
        );
        ensure!(
            !matches!(self, Self::Monthly { day, .. } if *day != -1 && !(1..=31).contains(day)),
            "invalid day of month"
        );
        Ok(())
    }

    pub fn next(&self, timezone: &str) -> Result<String> {
        let zone = Tz::from_str(timezone).map_err(|_| anyhow!("invalid timezone: {timezone}"))?;
        let now = Utc::now().with_timezone(&zone);
        let candidate = match *self {
            Self::Interval { value, unit } => {
                now + match unit {
                    IntervalUnit::Hours => Duration::hours(value),
                    IntervalUnit::Days => Duration::days(value),
                }
            }
            Self::Daily { hour, minute } => {
                let today = local(&zone, now.year(), now.month(), now.day(), hour, minute)?;
                if today > now {
                    today
                } else {
                    local_date(&zone, now.date_naive() + Duration::days(1), hour, minute)?
                }
            }
            Self::Weekly {
                weekday,
                hour,
                minute,
            } => {
                let target = [
                    Weekday::Mon,
                    Weekday::Tue,
                    Weekday::Wed,
                    Weekday::Thu,
                    Weekday::Fri,
                    Weekday::Sat,
                    Weekday::Sun,
                ][weekday as usize];
                let mut days = (target.num_days_from_monday() as i64
                    - now.weekday().num_days_from_monday() as i64)
                    .rem_euclid(7);
                let mut value =
                    local_date(&zone, now.date_naive() + Duration::days(days), hour, minute)?;
                if value <= now {
                    days += 7;
                    value =
                        local_date(&zone, now.date_naive() + Duration::days(days), hour, minute)?;
                }
                value
            }
            Self::Monthly { day, hour, minute } => {
                let (mut year, mut month) = (now.year(), now.month());
                let mut value = monthly(&zone, year, month, day, hour, minute)?;
                if value <= now {
                    if month == 12 {
                        year += 1;
                        month = 1;
                    } else {
                        month += 1;
                    }
                    value = monthly(&zone, year, month, day, hour, minute)?;
                }
                value
            }
        };
        Ok(candidate.with_timezone(&Utc).to_rfc3339())
    }
}

fn monthly(
    zone: &Tz,
    year: i32,
    month: u32,
    day: i32,
    hour: u32,
    minute: u32,
) -> Result<chrono::DateTime<Tz>> {
    let next = if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1)
    }
    .expect("valid next month");
    let last = (next - Duration::days(1)).day();
    local(
        zone,
        year,
        month,
        if day == -1 {
            last
        } else {
            (day as u32).min(last)
        },
        hour,
        minute,
    )
}

fn local_date(zone: &Tz, date: NaiveDate, hour: u32, minute: u32) -> Result<chrono::DateTime<Tz>> {
    local(zone, date.year(), date.month(), date.day(), hour, minute)
}

fn local(
    zone: &Tz,
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
) -> Result<chrono::DateTime<Tz>> {
    match zone.with_ymd_and_hms(year, month, day, hour, minute, 0) {
        LocalResult::Single(value) => Ok(value),
        LocalResult::Ambiguous(first, _) => Ok(first),
        LocalResult::None => {
            match zone.with_ymd_and_hms(year, month, day, (hour + 1).min(23), minute, 0) {
                LocalResult::Single(value) | LocalResult::Ambiguous(value, _) => Ok(value),
                LocalResult::None => Err(anyhow!("schedule time does not exist in {zone}")),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_all_schedule_shapes() {
        assert!(
            Cadence::Daily {
                hour: 24,
                minute: 0
            }
            .validate()
            .is_err()
        );
        assert!(
            Cadence::Weekly {
                weekday: 7,
                hour: 4,
                minute: 0
            }
            .validate()
            .is_err()
        );
        assert!(
            Cadence::Monthly {
                day: -1,
                hour: 4,
                minute: 0
            }
            .validate()
            .is_ok()
        );
        assert!(
            Cadence::Interval {
                value: 12,
                unit: IntervalUnit::Hours
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn next_run_is_utc_and_future() {
        let value = Cadence::Daily { hour: 4, minute: 0 }
            .next("America/New_York")
            .unwrap();
        assert!(chrono::DateTime::parse_from_rfc3339(&value).unwrap() > Utc::now());
    }
}
