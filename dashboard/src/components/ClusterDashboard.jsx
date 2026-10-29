import { useEffect, useState } from "react";
import { clusterStatus, ringStats } from "../api";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell,
} from "recharts";
import { Activity, HardDrive, Server, AlertTriangle, CheckCircle2, RefreshCw } from "lucide-react";

function StatusBadge({ status }) {
  if (status === "ACTIVE")
    return <span className="inline-flex items-center gap-1 text-xs text-green-400 bg-green-900/40 px-2 py-0.5 rounded-full"><CheckCircle2 className="w-3 h-3"/>ACTIVE</span>;
  if (status === "FAILED")
    return <span className="inline-flex items-center gap-1 text-xs text-red-400 bg-red-900/40 px-2 py-0.5 rounded-full"><AlertTriangle className="w-3 h-3"/>FAILED</span>;
  return <span className="text-xs text-gray-500 bg-gray-800 px-2 py-0.5 rounded-full">{status ?? "UNKNOWN"}</span>;
}

function StatCard({ label, value, sub, icon: Icon, color = "blue" }) {
  const colors = {
    blue:   "from-blue-900/40 to-blue-800/20 border-blue-800/50",
    green:  "from-green-900/40 to-green-800/20 border-green-800/50",
    red:    "from-red-900/40 to-red-800/20 border-red-800/50",
    yellow: "from-yellow-900/40 to-yellow-800/20 border-yellow-800/50",
  };
  return (
    <div className={`bg-gradient-to-br ${colors[color]} border rounded-xl p-4 flex items-center gap-4`}>
      <div className="p-2 bg-gray-900/60 rounded-lg">
        <Icon className={`w-6 h-6 text-${color}-400`} />
      </div>
      <div>
        <p className="text-2xl font-bold text-white">{value}</p>
        <p className="text-sm text-gray-400">{label}</p>
        {sub && <p className="text-xs text-gray-500">{sub}</p>}
      </div>
    </div>
  );
}

const NODE_COLORS = ["#3b82f6","#10b981","#f59e0b","#ec4899"];

export default function ClusterDashboard() {
  const [data, setData]         = useState(null);
  const [ring, setRing]         = useState(null);
  const [loading, setLoading]   = useState(true);
  const [lastRefresh, setLast]  = useState(null);

  const load = async () => {
    setLoading(true);
    try {
      const [cs, rs] = await Promise.all([clusterStatus(), ringStats()]);
      setData(cs.data);
      setRing(rs.data);
      setLast(new Date().toLocaleTimeString());
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
    const t = setInterval(load, 10000);
    return () => clearInterval(t);
  }, []);

  if (loading && !data)
    return <div className="flex items-center justify-center h-64 text-gray-500">Loading cluster data…</div>;

  const summary = data?.summary || {};
  const nodes   = data?.nodes   || [];
  const degraded= data?.degraded_objects || {};

  const ringData = ring
    ? Object.entries(ring.distribution || {}).map(([id, v]) => ({ id, chunks: v.chunks, pct: v.pct }))
    : [];

  const totalFreeGB = nodes.reduce((s, n) => s + (n.free_space_gb || 0), 0).toFixed(1);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white flex items-center gap-2">
            <Activity className="w-6 h-6 text-blue-400" /> Cluster Telemetry
          </h1>
          {lastRefresh && <p className="text-xs text-gray-500 mt-0.5">Last refresh: {lastRefresh} · auto-refreshes every 10s</p>}
        </div>
        <button onClick={load} className="flex items-center gap-2 text-sm text-gray-400 hover:text-white bg-gray-800 hover:bg-gray-700 px-3 py-1.5 rounded-lg transition">
          <RefreshCw className="w-4 h-4" /> Refresh
        </button>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard label="Total Nodes"  value={summary.total_nodes  ?? "-"} icon={Server}    color="blue"/>
        <StatCard label="Active Nodes" value={summary.active_nodes ?? "-"} icon={CheckCircle2} color="green"/>
        <StatCard label="Failed Nodes" value={summary.failed_nodes ?? "-"} icon={AlertTriangle} color={summary.failed_nodes > 0 ? "red" : "green"}/>
        <StatCard label="Total Chunks" value={summary.total_chunks ?? "-"} sub={`${totalFreeGB} GB free`} icon={HardDrive} color="yellow"/>
      </div>

      {/* Node grid */}
      <div>
        <h2 className="text-lg font-semibold text-white mb-3">Storage Nodes</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
          {nodes.map((n, i) => (
            <div key={n.node_id} className="bg-gray-900 border border-gray-800 rounded-xl p-4 space-y-3">
              <div className="flex items-center justify-between">
                <span className="font-semibold text-white">{n.node_id}</span>
                <StatusBadge status={n.status} />
              </div>
              <p className="text-xs text-gray-500 font-mono truncate">{n.url}</p>
              {/* Disk bar */}
              <div>
                <div className="flex justify-between text-xs text-gray-400 mb-1">
                  <span>Disk free</span>
                  <span className="text-white">{(n.free_space_gb || 0).toFixed(1)} GB</span>
                </div>
                <div className="w-full bg-gray-800 rounded-full h-1.5">
                  <div
                    className="h-1.5 rounded-full"
                    style={{
                      width: `${Math.min(100, ((n.free_space_gb || 0) / 1000) * 100)}%`,
                      backgroundColor: NODE_COLORS[i % NODE_COLORS.length],
                    }}
                  />
                </div>
              </div>
              <div className="flex justify-between text-xs">
                <span className="text-gray-400">Chunks</span>
                <span className="text-white font-mono">{n.chunk_count ?? 0}</span>
              </div>
              {n.last_seen && (
                <p className="text-xs text-gray-600">
                  Last seen: {new Date(n.last_seen).toLocaleTimeString()}
                </p>
              )}
            </div>
          ))}
        </div>
      </div>

      {/* Ring distribution chart */}
      {ringData.length > 0 && (
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
          <h2 className="text-lg font-semibold text-white mb-4">Chunk Distribution (Hash Ring)</h2>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={ringData} barSize={40}>
              <XAxis dataKey="id" tick={{ fill: "#9ca3af", fontSize: 12 }} />
              <YAxis tick={{ fill: "#9ca3af", fontSize: 12 }} />
              <Tooltip
                contentStyle={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 8 }}
                labelStyle={{ color: "#fff" }}
                itemStyle={{ color: "#9ca3af" }}
              />
              <Bar dataKey="chunks" radius={[4, 4, 0, 0]}>
                {ringData.map((_, i) => (
                  <Cell key={i} fill={NODE_COLORS[i % NODE_COLORS.length]} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          <div className="flex gap-4 mt-3 flex-wrap">
            {ringData.map((n, i) => (
              <div key={n.id} className="flex items-center gap-1.5 text-xs text-gray-400">
                <span className="w-2.5 h-2.5 rounded-full" style={{ background: NODE_COLORS[i % NODE_COLORS.length] }} />
                {n.id}: {n.pct}%
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Degraded objects */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
        <h2 className="text-lg font-semibold text-white mb-3 flex items-center gap-2">
          <AlertTriangle className="w-5 h-5 text-yellow-400" />
          Degraded Objects
          <span className={`ml-2 text-sm px-2 py-0.5 rounded-full ${degraded.count > 0 ? "bg-red-900/40 text-red-400" : "bg-green-900/40 text-green-400"}`}>
            {degraded.count ?? 0}
          </span>
        </h2>
        {degraded.count > 0 ? (
          <table className="w-full text-sm text-gray-300">
            <thead><tr className="text-left text-gray-500 border-b border-gray-800">
              <th className="pb-2">Bucket</th><th className="pb-2">Key</th><th className="pb-2">Affected Chunks</th>
            </tr></thead>
            <tbody>
              {degraded.objects?.map((o, i) => (
                <tr key={i} className="border-b border-gray-800/50 hover:bg-gray-800/40">
                  <td className="py-2">{o.bucket}</td>
                  <td className="py-2 font-mono text-xs">{o.key}</td>
                  <td className="py-2 text-red-400 text-xs">{o.affected_chunks?.join(", ")}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p className="text-green-400 text-sm flex items-center gap-2"><CheckCircle2 className="w-4 h-4" /> All objects are fully replicated.</p>
        )}
      </div>
    </div>
  );
}
