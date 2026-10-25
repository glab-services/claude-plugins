// Match bytes exactly: no trimming, Unicode aliases, empty or dot segments.
const segment = String.raw`(?!\.{1,2}(?:/|$))[a-zA-Z0-9_.-]{1,64}`;
const topic = String.raw`(?=.{1,128}$)${segment}(?:/${segment})*`;
export const topicPattern = new RegExp(`^${topic}$(?![\\s\\S])`);
export const addressPattern = new RegExp(`^(?:(?:ws|user):[a-zA-Z0-9_.-]{1,128}|topic:${topic})$(?![\\s\\S])`);
