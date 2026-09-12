interface MessageLike {
  readonly text: string;
  readonly attachments: ReadonlyArray<unknown>;
}

function isAttachmentFreeCommand(message: MessageLike, command: string): boolean {
  return message.attachments.length === 0 && message.text.trim().toLowerCase() === command;
}

export function isCompactCommand(message: MessageLike): boolean {
  return isAttachmentFreeCommand(message, "/compact");
}

export function isLogoutCommand(message: MessageLike): boolean {
  return isAttachmentFreeCommand(message, "/logout");
}

export function isNativeMaintenanceCommand(message: MessageLike): boolean {
  return isCompactCommand(message) || isLogoutCommand(message);
}
