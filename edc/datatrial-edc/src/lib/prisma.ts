import { PrismaClient } from "@prisma/client";
import { PrismaBetterSqlite3 } from "@prisma/adapter-better-sqlite3";
import path from "path";

const globalForPrisma = globalThis as unknown as {
  prisma: PrismaClient | undefined;
};

const getDatabasePath = () => {
  return path.join(process.cwd(), "prisma", "dev.db");
};

const createPrismaClient = () => {
  const adapter = new PrismaBetterSqlite3({
    url: `file:${getDatabasePath()}`,
  });
  return new PrismaClient({ adapter });
};

export const prisma = globalForPrisma.prisma ?? createPrismaClient();

if (process.env.NODE_ENV !== "production") globalForPrisma.prisma = prisma;

export default prisma;
