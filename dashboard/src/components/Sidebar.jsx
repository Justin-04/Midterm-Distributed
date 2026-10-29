import { Database, Server, Search, LogOut } from "lucide-react";

const NAV = [
  { id: "cluster", label: "Cluster",  icon: Server },
  { id: "buckets", label: "Explorer", icon: Database },
  { id: "search",  label: "Search",   icon: Search },
];

export default function Sidebar({ current, onChange, onLogout }) {
  return (
    <aside className="w-56 bg-gray-900 border-r border-gray-800 flex flex-col">
      {/* Logo */}
      <div className="px-5 py-5 border-b border-gray-800">
        <p className="text-white font-bold text-lg">DistStore</p>
        <p className="text-gray-500 text-xs">Storage Dashboard</p>
      </div>

      {/* Nav */}
      <nav className="flex-1 py-4 space-y-1 px-3">
        {NAV.map(({ id, label, icon: Icon }) => (
          <button
            key={id}
            onClick={() => onChange(id)}
            className={`w-full flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition
              ${current === id
                ? "bg-blue-600 text-white"
                : "text-gray-400 hover:bg-gray-800 hover:text-white"}`}
          >
            <Icon className="w-4 h-4" />
            {label}
          </button>
        ))}
      </nav>

      {/* Logout */}
      <div className="p-3 border-t border-gray-800">
        <button
          onClick={onLogout}
          className="w-full flex items-center gap-3 px-3 py-2 rounded-lg text-sm text-gray-400 hover:bg-gray-800 hover:text-white transition"
        >
          <LogOut className="w-4 h-4" /> Sign Out
        </button>
      </div>
    </aside>
  );
}
