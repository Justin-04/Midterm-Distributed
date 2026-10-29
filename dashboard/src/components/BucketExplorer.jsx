import { useEffect, useState, useCallback } from "react";
import { useDropzone } from "react-dropzone";
import {
  listBuckets, createBucket, deleteBucket,
  listObjects, uploadObject, deleteObject, downloadObject, getObjectMeta,
} from "../api";
import {
  FolderOpen, Plus, Trash2, Upload, Download, RefreshCw,
  File, ChevronRight, X, Tag, AlertTriangle,
} from "lucide-react";

function fmt(bytes) {
  if (bytes < 1024)       return `${bytes} B`;
  if (bytes < 1024**2)    return `${(bytes/1024).toFixed(1)} KB`;
  if (bytes < 1024**3)    return `${(bytes/1024**2).toFixed(1)} MB`;
  return `${(bytes/1024**3).toFixed(2)} GB`;
}

// ── Create Bucket Modal ───────────────────────────────────────────────────────
function CreateBucketModal({ onClose, onCreated }) {
  const [name,    setName]    = useState("");
  const [enc,     setEnc]     = useState("replication");
  const [n,       setN]       = useState(3);
  const [w,       setW]       = useState(2);
  const [r,       setR]       = useState(2);
  const [k,       setK]       = useState(2);
  const [m,       setM]       = useState(2);
  const [err,     setErr]     = useState("");
  const [loading, setLoading] = useState(false);

  const submit = async (e) => {
    e.preventDefault();
    setErr(""); setLoading(true);
    try {
      await createBucket(name, {
        encoding: enc, replication_factor: +n,
        write_quorum: +w, read_quorum: +r,
        erasure_k: +k, erasure_m: +m,
      });
      onCreated();
      onClose();
    } catch (ex) {
      setErr(ex.response?.data?.detail || "Failed to create bucket");
    } finally { setLoading(false); }
  };

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50">
      <div className="bg-gray-900 border border-gray-700 rounded-2xl p-6 w-full max-w-md shadow-2xl">
        <div className="flex justify-between items-center mb-5">
          <h2 className="text-lg font-semibold text-white">New Bucket</h2>
          <button onClick={onClose}><X className="w-5 h-5 text-gray-400 hover:text-white"/></button>
        </div>
        <form onSubmit={submit} className="space-y-4">
          <div>
            <label className="text-sm text-gray-400">Bucket name</label>
            <input required value={name} onChange={e=>setName(e.target.value)}
              className="mt-1 w-full bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-white text-sm focus:outline-none focus:border-blue-500" />
          </div>
          <div>
            <label className="text-sm text-gray-400">Encoding</label>
            <select value={enc} onChange={e=>setEnc(e.target.value)}
              className="mt-1 w-full bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-white text-sm focus:outline-none">
              <option value="replication">Replication</option>
              <option value="erasure">Erasure Coding</option>
            </select>
          </div>
          {enc === "replication" && (
            <div className="grid grid-cols-3 gap-3">
              {[["N (replicas)", n, setN], ["W (write Q)", w, setW], ["R (read Q)", r, setR]].map(([l, v, s]) => (
                <div key={l}>
                  <label className="text-xs text-gray-400">{l}</label>
                  <input type="number" min={1} max={10} value={v} onChange={e=>s(e.target.value)}
                    className="mt-1 w-full bg-gray-800 border border-gray-700 rounded-lg px-2 py-1.5 text-white text-sm focus:outline-none" />
                </div>
              ))}
            </div>
          )}
          {enc === "erasure" && (
            <div className="grid grid-cols-2 gap-3">
              {[["k (data)", k, setK], ["m (parity)", m, setM]].map(([l, v, s]) => (
                <div key={l}>
                  <label className="text-xs text-gray-400">{l}</label>
                  <input type="number" min={1} max={10} value={v} onChange={e=>s(e.target.value)}
                    className="mt-1 w-full bg-gray-800 border border-gray-700 rounded-lg px-2 py-1.5 text-white text-sm focus:outline-none" />
                </div>
              ))}
            </div>
          )}
          {err && <p className="text-red-400 text-sm">{err}</p>}
          <button type="submit" disabled={loading}
            className="w-full bg-blue-600 hover:bg-blue-500 disabled:opacity-50 text-white font-semibold rounded-lg py-2 text-sm transition">
            {loading ? "Creating…" : "Create Bucket"}
          </button>
        </form>
      </div>
    </div>
  );
}

// ── Upload Drop Zone ──────────────────────────────────────────────────────────
function UploadZone({ bucket, onDone }) {
  const [progress, setProgress] = useState({});
  const [tags, setTags]         = useState("");

  const onDrop = useCallback(async (files) => {
    for (const file of files) {
      const key = file.name;
      setProgress(p => ({ ...p, [key]: 0 }));
      try {
        await uploadObject(bucket, key, file, tags || undefined, (pct) =>
          setProgress(p => ({ ...p, [key]: pct }))
        );
        setProgress(p => ({ ...p, [key]: 100 }));
        onDone();
      } catch {
        setProgress(p => ({ ...p, [key]: -1 }));
      }
    }
  }, [bucket, tags, onDone]);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({ onDrop });

  return (
    <div className="space-y-3">
      <div>
        <label className="text-xs text-gray-400 flex items-center gap-1 mb-1"><Tag className="w-3 h-3"/>Tags (key=val,key=val)</label>
        <input value={tags} onChange={e=>setTags(e.target.value)} placeholder="env=prod,team=ops"
          className="w-full bg-gray-800 border border-gray-700 rounded-lg px-3 py-1.5 text-white text-sm focus:outline-none focus:border-blue-500"/>
      </div>
      <div {...getRootProps()} className={`border-2 border-dashed rounded-xl p-6 text-center cursor-pointer transition
          ${isDragActive ? "border-blue-500 bg-blue-900/20" : "border-gray-700 hover:border-gray-600"}`}>
        <input {...getInputProps()} />
        <Upload className="w-8 h-8 text-gray-500 mx-auto mb-2" />
        <p className="text-sm text-gray-400">{isDragActive ? "Drop files here…" : "Drag & drop files, or click to select"}</p>
        <p className="text-xs text-gray-600 mt-1">Supports any file type · multipart for large files</p>
      </div>
      {/* Progress bars */}
      {Object.entries(progress).map(([name, pct]) => (
        <div key={name} className="bg-gray-800 rounded-lg p-3">
          <div className="flex justify-between text-xs text-gray-400 mb-1">
            <span className="font-mono truncate max-w-xs">{name}</span>
            <span>{pct < 0 ? "❌ Failed" : pct === 100 ? "✅ Done" : `${pct}%`}</span>
          </div>
          <div className="w-full bg-gray-700 rounded-full h-1.5">
            <div className={`h-1.5 rounded-full transition-all ${pct < 0 ? "bg-red-500" : pct === 100 ? "bg-green-500" : "bg-blue-500"}`}
              style={{ width: `${Math.max(0, pct)}%` }} />
          </div>
        </div>
      ))}
    </div>
  );
}

// ── Object row ────────────────────────────────────────────────────────────────
function ObjectRow({ bucket, obj, onDeleted }) {
  const [deleting,   setDeleting]  = useState(false);
  const [showMeta,   setShowMeta]  = useState(false);
  const [meta,       setMeta]      = useState(null);

  const handleDownload = async () => {
    const res = await downloadObject(bucket, obj.key);
    const url = URL.createObjectURL(res.data);
    const a   = document.createElement("a");
    a.href = url; a.download = obj.key.split("/").pop();
    a.click(); URL.revokeObjectURL(url);
  };

  const handleDelete = async () => {
    if (!confirm(`Delete ${obj.key}?`)) return;
    setDeleting(true);
    try { await deleteObject(bucket, obj.key); onDeleted(); }
    finally { setDeleting(false); }
  };

  const handleMeta = async () => {
    if (!meta) {
      const res = await getObjectMeta(bucket, obj.key);
      setMeta(res.data);
    }
    setShowMeta(!showMeta);
  };

  return (
    <>
      <tr className="border-b border-gray-800/50 hover:bg-gray-800/30 transition">
        <td className="py-2 px-3">
          <button onClick={handleMeta} className="flex items-center gap-2 text-left">
            <File className="w-4 h-4 text-blue-400 shrink-0" />
            <span className="font-mono text-sm text-white hover:text-blue-400 truncate max-w-xs">{obj.key}</span>
            <ChevronRight className={`w-3 h-3 text-gray-500 transition-transform ${showMeta ? "rotate-90" : ""}`} />
          </button>
        </td>
        <td className="py-2 px-3 text-sm text-gray-400">{fmt(obj.size)}</td>
        <td className="py-2 px-3 text-sm text-gray-400">{obj.chunk_count}</td>
        <td className="py-2 px-3 text-xs text-gray-500">{obj.creation_date ? new Date(obj.creation_date).toLocaleString() : "—"}</td>
        <td className="py-2 px-3">
          <div className="flex gap-1 flex-wrap">
            {Object.entries(obj.tags || {}).map(([k, v]) => (
              <span key={k} className="text-xs bg-blue-900/40 text-blue-300 px-1.5 py-0.5 rounded">{k}={v}</span>
            ))}
          </div>
        </td>
        <td className="py-2 px-3">
          <div className="flex gap-2">
            <button onClick={handleDownload} className="text-gray-400 hover:text-white" title="Download">
              <Download className="w-4 h-4" />
            </button>
            <button onClick={handleDelete} disabled={deleting} className="text-gray-400 hover:text-red-400" title="Delete">
              <Trash2 className="w-4 h-4" />
            </button>
          </div>
        </td>
      </tr>
      {showMeta && meta && (
        <tr className="bg-gray-900/60">
          <td colSpan={6} className="px-6 py-3">
            <div className="text-xs text-gray-400 space-y-1 font-mono">
              <p><span className="text-gray-500">encoding:</span> {meta.encoding}</p>
              <p><span className="text-gray-500">version:</span>  {meta.version}</p>
              <p><span className="text-gray-500">hashes:</span></p>
              <ul className="pl-4 space-y-0.5">
                {meta.chunk_hashes?.map(h => <li key={h} className="text-gray-500">{h}</li>)}
              </ul>
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

// ── Main Explorer ─────────────────────────────────────────────────────────────
export default function BucketExplorer() {
  const [buckets,      setBuckets]      = useState([]);
  const [selected,     setSelected]     = useState(null);
  const [objects,      setObjects]      = useState([]);
  const [showCreate,   setShowCreate]   = useState(false);
  const [showUpload,   setShowUpload]   = useState(false);
  const [loading,      setLoading]      = useState(false);
  const [prefix,       setPrefix]       = useState("");

  const loadBuckets = async () => {
    const res = await listBuckets();
    setBuckets(res.data.buckets || []);
  };

  const loadObjects = async (b = selected) => {
    if (!b) return;
    setLoading(true);
    try {
      const res = await listObjects(b, prefix || undefined);
      setObjects(res.data.objects || []);
    } finally { setLoading(false); }
  };

  useEffect(() => { loadBuckets(); }, []);
  useEffect(() => { loadObjects(); }, [selected, prefix]);

  const handleDeleteBucket = async (b) => {
    if (!confirm(`Delete bucket "${b}"?`)) return;
    await deleteBucket(b, true);
    if (selected === b) { setSelected(null); setObjects([]); }
    loadBuckets();
  };

  return (
    <div className="space-y-5">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-white flex items-center gap-2">
          <FolderOpen className="w-6 h-6 text-blue-400" /> Bucket Explorer
        </h1>
        <button onClick={() => setShowCreate(true)}
          className="flex items-center gap-2 bg-blue-600 hover:bg-blue-500 text-white text-sm px-3 py-1.5 rounded-lg transition">
          <Plus className="w-4 h-4" /> New Bucket
        </button>
      </div>

      <div className="flex gap-5">
        {/* Bucket list */}
        <div className="w-56 shrink-0 space-y-1">
          <p className="text-xs text-gray-500 uppercase tracking-wider mb-2">Buckets ({buckets.length})</p>
          {buckets.map(b => (
            <div key={b.bucket}
              className={`flex items-center justify-between px-3 py-2 rounded-lg cursor-pointer transition group
                ${selected === b.bucket ? "bg-blue-700/40 text-white" : "hover:bg-gray-800 text-gray-400"}`}
              onClick={() => setSelected(b.bucket)}>
              <div className="flex items-center gap-2 min-w-0">
                <FolderOpen className="w-4 h-4 shrink-0" />
                <span className="text-sm truncate">{b.bucket}</span>
              </div>
              <button onClick={e => { e.stopPropagation(); handleDeleteBucket(b.bucket); }}
                className="opacity-0 group-hover:opacity-100 text-gray-500 hover:text-red-400 transition">
                <Trash2 className="w-3.5 h-3.5" />
              </button>
            </div>
          ))}
          {buckets.length === 0 && (
            <p className="text-xs text-gray-600 px-2">No buckets yet. Create one above.</p>
          )}
        </div>

        {/* Objects panel */}
        <div className="flex-1 space-y-4">
          {selected ? (
            <>
              {/* Toolbar */}
              <div className="flex items-center gap-3">
                <input value={prefix} onChange={e => setPrefix(e.target.value)} placeholder="Filter by prefix…"
                  className="flex-1 bg-gray-800 border border-gray-700 rounded-lg px-3 py-1.5 text-sm text-white focus:outline-none focus:border-blue-500" />
                <button onClick={() => loadObjects()}
                  className="text-gray-400 hover:text-white bg-gray-800 px-2 py-1.5 rounded-lg">
                  <RefreshCw className="w-4 h-4" />
                </button>
                <button onClick={() => setShowUpload(!showUpload)}
                  className="flex items-center gap-2 bg-green-700 hover:bg-green-600 text-white text-sm px-3 py-1.5 rounded-lg transition">
                  <Upload className="w-4 h-4" /> Upload
                </button>
              </div>

              {showUpload && (
                <div className="bg-gray-900 border border-gray-800 rounded-xl p-4">
                  <h3 className="text-sm font-semibold text-white mb-3">Upload to <span className="text-blue-400">{selected}</span></h3>
                  <UploadZone bucket={selected} onDone={() => { loadObjects(); setShowUpload(false); }} />
                </div>
              )}

              {/* Objects table */}
              <div className="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden">
                <div className="px-4 py-3 border-b border-gray-800 flex items-center justify-between">
                  <span className="text-sm font-semibold text-white">{selected}</span>
                  <span className="text-xs text-gray-500">{objects.length} object(s)</span>
                </div>
                {loading ? (
                  <div className="p-8 text-center text-gray-500">Loading…</div>
                ) : objects.length === 0 ? (
                  <div className="p-8 text-center text-gray-500">No objects in this bucket.</div>
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="text-left text-xs text-gray-500 border-b border-gray-800">
                          <th className="px-3 py-2">Key</th>
                          <th className="px-3 py-2">Size</th>
                          <th className="px-3 py-2">Chunks</th>
                          <th className="px-3 py-2">Created</th>
                          <th className="px-3 py-2">Tags</th>
                          <th className="px-3 py-2">Actions</th>
                        </tr>
                      </thead>
                      <tbody>
                        {objects.map(o => (
                          <ObjectRow key={o.key} bucket={selected} obj={o} onDeleted={() => loadObjects()} />
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            </>
          ) : (
            <div className="flex flex-col items-center justify-center h-64 text-gray-600 space-y-2">
              <FolderOpen className="w-12 h-12" />
              <p>Select a bucket to browse objects</p>
            </div>
          )}
        </div>
      </div>

      {showCreate && <CreateBucketModal onClose={() => setShowCreate(false)} onCreated={loadBuckets} />}
    </div>
  );
}
