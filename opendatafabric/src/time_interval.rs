use chrono::prelude::*;
use intervals_general::bound_pair::BoundPair;
use intervals_general::interval::Interval;

use serde::de::{Deserialize, Deserializer, Error, Visitor};
use serde::{Serialize, Serializer};
use std::convert::TryFrom;
use std::fmt;

type Element = DateTime<Utc>;

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct TimeInterval(pub Interval<Element>);

#[derive(Debug, Clone)]
pub struct InvalidTimeInterval;

impl fmt::Display for InvalidTimeInterval {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "invalid time interval")
    }
}

impl TimeInterval {
    pub fn empty() -> Self {
        Self(Interval::Empty)
    }

    pub fn closed(left: Element, right: Element) -> Result<Self, InvalidTimeInterval> {
        match BoundPair::new(left, right) {
            None => Err(InvalidTimeInterval),
            Some(p) => Ok(Self(Interval::Closed { bound_pair: p })),
        }
    }

    pub fn open(left: Element, right: Element) -> Result<Self, InvalidTimeInterval> {
        match BoundPair::new(left, right) {
            None => Err(InvalidTimeInterval),
            Some(p) => Ok(Self(Interval::Open { bound_pair: p })),
        }
    }

    pub fn left_half_open(left: Element, right: Element) -> Result<Self, InvalidTimeInterval> {
        match BoundPair::new(left, right) {
            None => Err(InvalidTimeInterval),
            Some(p) => Ok(Self(Interval::LeftHalfOpen { bound_pair: p })),
        }
    }

    pub fn right_half_open(left: Element, right: Element) -> Result<Self, InvalidTimeInterval> {
        match BoundPair::new(left, right) {
            None => Err(InvalidTimeInterval),
            Some(p) => Ok(Self(Interval::RightHalfOpen { bound_pair: p })),
        }
    }

    pub fn unbounded_closed_right(right: Element) -> Self {
        Self(Interval::UnboundedClosedRight { right: right })
    }

    pub fn unbounded_open_right(right: Element) -> Self {
        Self(Interval::UnboundedOpenRight { right: right })
    }

    pub fn unbounded_closed_left(left: Element) -> Self {
        Self(Interval::UnboundedClosedLeft { left: left })
    }

    pub fn unbounded_open_left(left: Element) -> Self {
        Self(Interval::UnboundedOpenLeft { left: left })
    }

    pub fn singleton(at: Element) -> Self {
        Self(Interval::Singleton { at: at })
    }

    pub fn unbounded() -> Self {
        Self(Interval::Unbounded)
    }

    pub fn is_empty(&self) -> bool {
        match self.0 {
            Interval::Empty => true,
            _ => false,
        }
    }

    // TODO: upstream
    pub fn left_complement(&self) -> TimeInterval {
        match self.0 {
            Interval::Empty => Self(Interval::Unbounded),
            Interval::Unbounded => Self(Interval::Empty),
            Interval::Singleton { at } => Self::unbounded_open_right(at),
            Interval::Open { bound_pair } => {
                Self::unbounded_closed_right(bound_pair.left().clone())
            }
            Interval::Closed { bound_pair } => {
                Self::unbounded_open_right(bound_pair.left().clone())
            }
            Interval::LeftHalfOpen { bound_pair } => {
                Self::unbounded_closed_right(bound_pair.left().clone())
            }
            Interval::RightHalfOpen { bound_pair } => {
                Self::unbounded_open_right(bound_pair.left().clone())
            }
            Interval::UnboundedOpenRight { .. } => Self(Interval::Empty),
            Interval::UnboundedClosedRight { .. } => Self(Interval::Empty),
            Interval::UnboundedOpenLeft { left } => Self::unbounded_closed_right(left.clone()),
            Interval::UnboundedClosedLeft { left } => Self::unbounded_open_right(left.clone()),
        }
    }

    pub fn right_complement(&self) -> TimeInterval {
        match self.0 {
            Interval::Empty => Self(Interval::Unbounded),
            Interval::Unbounded => Self(Interval::Empty),
            Interval::Singleton { at } => Self::unbounded_open_left(at),
            Interval::Open { bound_pair } => {
                Self::unbounded_closed_left(bound_pair.right().clone())
            }
            Interval::Closed { bound_pair } => {
                Self::unbounded_open_left(bound_pair.right().clone())
            }
            Interval::LeftHalfOpen { bound_pair } => {
                Self::unbounded_open_left(bound_pair.right().clone())
            }
            Interval::RightHalfOpen { bound_pair } => {
                Self::unbounded_closed_left(bound_pair.right().clone())
            }
            Interval::UnboundedOpenRight { right } => Self::unbounded_closed_left(right.clone()),
            Interval::UnboundedClosedRight { right } => Self::unbounded_open_left(right.clone()),
            Interval::UnboundedOpenLeft { .. } => Self(Interval::Empty),
            Interval::UnboundedClosedLeft { .. } => Self(Interval::Empty),
        }
    }

    pub fn contains(&self, other: &TimeInterval) -> bool {
        self.0.contains(&other.0)
    }

    pub fn contains_point(&self, point: &Element) -> bool {
        self.contains(&TimeInterval::singleton(point.clone()))
    }

    pub fn intersect(&self, other: &TimeInterval) -> TimeInterval {
        Self(self.0.intersect(&other.0))
    }
}

impl Eq for TimeInterval {}

impl fmt::Display for TimeInterval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn fmt_elem(v: &Element) -> String {
            v.to_rfc3339_opts(SecondsFormat::Millis, true)
        }

        match self.0 {
            Interval::Unbounded => f.write_str("(-inf, inf)"),
            Interval::Empty => f.write_str("()"),
            Interval::Closed { bound_pair: p } => {
                write!(f, "[{}, {}]", fmt_elem(p.left()), fmt_elem(p.right()))
            }
            Interval::Open { bound_pair: p } => {
                write!(f, "({}, {})", fmt_elem(p.left()), fmt_elem(p.right()))
            }
            Interval::LeftHalfOpen { bound_pair: p } => {
                write!(f, "({}, {}]", fmt_elem(p.left()), fmt_elem(p.right()))
            }
            Interval::RightHalfOpen { bound_pair: p } => {
                write!(f, "[{}, {})", fmt_elem(p.left()), fmt_elem(p.right()))
            }
            Interval::UnboundedClosedRight { right } => write!(f, "(-inf, {}]", fmt_elem(&right)),
            Interval::UnboundedOpenRight { right } => write!(f, "(-inf, {})", fmt_elem(&right)),
            Interval::UnboundedClosedLeft { left } => write!(f, "[{}, inf)", fmt_elem(&left)),
            Interval::UnboundedOpenLeft { left } => write!(f, "({}, inf)", fmt_elem(&left)),
            Interval::Singleton { at } => write!(f, "[{}, {}]", fmt_elem(&at), fmt_elem(&at)),
        }
    }
}

impl TryFrom<&str> for TimeInterval {
    type Error = InvalidTimeInterval;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value == "()" {
            return Ok(TimeInterval::empty());
        } else if value.len() < 3 {
            return Err(InvalidTimeInterval);
        }

        fn parse_dt(s: &str) -> Result<Element, InvalidTimeInterval> {
            DateTime::parse_from_rfc3339(s.trim())
                .map(|dt| dt.into())
                .map_err(|_| InvalidTimeInterval)
        }

        let lbound = &value[0..1];
        let rbound = &value[value.len() - 1..value.len()];
        let mut split = value[1..value.len() - 1].split(',');

        let left = match split.next() {
            None => return Err(InvalidTimeInterval),
            Some("-inf") => None,
            Some(v) => match parse_dt(v) {
                Ok(dt) => Some(dt),
                _ => return Err(InvalidTimeInterval),
            },
        };

        let right = match split.next() {
            None => return Err(InvalidTimeInterval),
            Some("inf") => None,
            Some(v) => match parse_dt(v) {
                Ok(dt) => Some(dt),
                _ => return Err(InvalidTimeInterval),
            },
        };

        if split.next() != None {
            return Err(InvalidTimeInterval);
        }

        match (lbound, left, right, rbound) {
            ("(", None, None, ")") => Ok(TimeInterval::unbounded()),
            ("(", None, Some(r), ")") => Ok(TimeInterval::unbounded_open_right(r)),
            ("(", None, Some(r), "]") => Ok(TimeInterval::unbounded_closed_right(r)),
            ("(", Some(l), None, ")") => Ok(TimeInterval::unbounded_open_left(l)),
            ("[", Some(l), None, ")") => Ok(TimeInterval::unbounded_closed_left(l)),
            ("(", Some(l), Some(r), ")") => TimeInterval::open(l, r),
            ("(", Some(l), Some(r), "]") => TimeInterval::left_half_open(l, r),
            ("[", Some(l), Some(r), ")") => TimeInterval::right_half_open(l, r),
            ("[", Some(l), Some(r), "]") => {
                if l == r {
                    Ok(TimeInterval::singleton(l))
                } else {
                    TimeInterval::closed(l, r)
                }
            }
            _ => Err(InvalidTimeInterval),
        }
    }
}

impl Serialize for TimeInterval {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

struct TimeIntervalVisitor;

impl<'de> Visitor<'de> for TimeIntervalVisitor {
    type Value = TimeInterval;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a time interval")
    }

    fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
        TimeInterval::try_from(v).map_err(serde::de::Error::custom)
    }
}

// This is the trait that informs Serde how to deserialize MyMap.
impl<'de> Deserialize<'de> for TimeInterval {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(TimeIntervalVisitor)
    }
}
