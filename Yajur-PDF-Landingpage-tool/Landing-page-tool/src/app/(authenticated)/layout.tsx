import { redirect } from "next/navigation";
import { getCurrentUser } from "@/lib/auth-helpers";
import { Header } from "@/components/ui/header";

export default async function AuthenticatedLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const result = await getCurrentUser();

  if (!result) {
    redirect("/login");
  }

  return (
    <div className="min-h-screen">
      <Header profile={result.profile} />
      <main>{children}</main>
    </div>
  );
}
