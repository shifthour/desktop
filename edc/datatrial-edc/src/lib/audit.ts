import prisma from "./prisma";

export interface AuditLogParams {
  tableName: string;
  recordId: string;
  action: "CREATE" | "UPDATE" | "DELETE" | "LOGIN" | "LOGOUT" | "SIGN" | "SDV" | "LOCK" | "UNLOCK";
  oldValues?: Record<string, unknown>;
  newValues?: Record<string, unknown>;
  changedFields?: string[];
  changeReason?: string;
  userId?: string;
  userEmail?: string;
  userRole?: string;
  ipAddress?: string;
  userAgent?: string;
  sessionId?: string;
}

export async function createAuditLog(params: AuditLogParams) {
  return prisma.auditLog.create({
    data: {
      tableName: params.tableName,
      recordId: params.recordId,
      action: params.action,
      oldValues: params.oldValues ? JSON.stringify(params.oldValues) : null,
      newValues: params.newValues ? JSON.stringify(params.newValues) : null,
      changedFields: params.changedFields ? params.changedFields.join(",") : null,
      changeReason: params.changeReason,
      userId: params.userId,
      userEmail: params.userEmail,
      userRole: params.userRole,
      ipAddress: params.ipAddress,
      userAgent: params.userAgent,
      sessionId: params.sessionId,
    },
  });
}

export function getChangedFields<T extends Record<string, unknown>>(
  oldValues: T,
  newValues: T
): { changedFields: string[]; oldObj: Partial<T>; newObj: Partial<T> } {
  const changedFields: string[] = [];
  const oldObj: Partial<T> = {};
  const newObj: Partial<T> = {};

  for (const key of Object.keys(newValues) as (keyof T)[]) {
    if (oldValues[key] !== newValues[key]) {
      changedFields.push(key as string);
      oldObj[key] = oldValues[key];
      newObj[key] = newValues[key];
    }
  }

  return { changedFields, oldObj, newObj };
}
