import { GENERIC_MIME_TYPES } from "./image.ts";

const VIDEO_MIME_TYPE_BY_EXTENSION = new Map([
  ["avi", "video/x-msvideo"],
  ["m4v", "video/mp4"],
  ["mkv", "video/x-matroska"],
  ["mov", "video/quicktime"],
  ["mp4", "video/mp4"],
  ["ogv", "video/ogg"],
  ["webm", "video/webm"],
]);

// Operating systems report these for `.ts` source files; browsers can't play them.
export const MPEG_TRANSPORT_STREAM_MIME_TYPES = new Set(["video/mp2t", "video/vnd.dlna.mpeg-tts"]);

export const VIDEO_FILE_EXTENSIONS = Object.freeze([...VIDEO_MIME_TYPE_BY_EXTENSION.keys()]);

export function videoMimeType(attachment: {
  readonly name: string;
  readonly mimeType: string;
}): string | null {
  const mimeType = attachment.mimeType.split(";", 1)[0]?.trim().toLowerCase() ?? "";
  const generic =
    GENERIC_MIME_TYPES.has(mimeType) || MPEG_TRANSPORT_STREAM_MIME_TYPES.has(mimeType);
  if (mimeType.startsWith("video/") && !generic) return mimeType;
  if (mimeType !== "" && !generic) return null;
  const dotIndex = attachment.name.lastIndexOf(".");
  return dotIndex < 0
    ? null
    : (VIDEO_MIME_TYPE_BY_EXTENSION.get(attachment.name.slice(dotIndex + 1).toLowerCase()) ?? null);
}
