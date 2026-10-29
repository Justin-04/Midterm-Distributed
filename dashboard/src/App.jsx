import { useState, useEffect } from "react";
import Login from "./components/Login";
import Sidebar from "./components/Sidebar";
import BucketExplorer from "./components/BucketExplorer";
import ClusterDashboard from "./components/ClusterDashboard";
import SearchPanel from "./components/SearchPanel";

export default function App() {
  const [token, setToken]   = useState(localStorage.getItem("jwt_token"));
  const [page, setPage]     = useState("cluster");

  const handleLogin = (t) => {
    localStorage.setItem("jwt_token", t);
    setToken(t);
  };

  const handleLogout = () => {
    localStorage.removeItem("jwt_token");
    setToken(null);
  };

  if (!token) return <Login onLogin={handleLogin} />;

  return (
    <div className="flex h-screen overflow-hidden">
      <Sidebar current={page} onChange={setPage} onLogout={handleLogout} />
      <main className="flex-1 overflow-y-auto p-6 bg-gray-950">
        {page === "cluster"  && <ClusterDashboard />}
        {page === "buckets"  && <BucketExplorer />}
        {page === "search"   && <SearchPanel />}
      </main>
    </div>
  );
}
