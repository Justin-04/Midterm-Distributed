import { useState } from "react";
import { search } from "../api";
import { Search, Tag, Calendar, HardDrive, ChevronDown, X } from "lucide-react";

function fmt(bytes) {
  if (!bytes) return "0 B";
  if (bytes < 1024)    return `${bytes} B`;
  if (bytes < 1024**2) return `${(bytes/1024).toFixed(1)} KB`;
  return `${(bytes/1024**2).toFixed(1)} MB`;
}

function Tag_(  { k, v, onRemove }) {
  return (
    <span className="inline-flex items-center gap-1 bg-blue-900/40 text-blue-300 text-xs px-2 py-0.5 rounded-full">
      {k}={v}
      {onRemove && <button onClick={onRemove}><X className="w-3 h-3"/></button>}
    </span>
  );
}

export default function SearchPanel() {
  const [bucket,    setBucket]    = useState("");
  const [prefix,    setPrefix]    = useState("");
  const [minSize,   setMinSize]   = useState("");
  const [maxSize,   setMaxSize]   = useState("");
  const [startDate, setStartDate] = useState("");
  const [endDate,   setEndDate]   = useState("");
  const [tagKey,    setTagKey]    = useState("");
  const [tagVal,    setTagVal]    = useState("");
  const [tags,      setTags]      = useState([]);
  const [results,   setResults]   = useState(null);
  const [loading,   setLoading]   = useState(false);
  const [error,     setError]     = useState("");

  const addTag = () => {
    if (tagKey && tagVal) {
      setTags(t => [...t, [tagKey, tagVal]]);
      setTagKey(""); setTagVal("");
    }
  };

  const removeTag = (i) => setTags(t => t.filter((_, idx) => idx !== i));

  const handleSearch = async (e) => {
    e?.preventDefault();
    setLoading(true); setError("");
    try {
      const params = {};
      if (bucket)    params.bucket    = bucket;
      if (prefix)    params.prefix    = prefix;
      if (minSize)   params.min_size  = +minSize;
      if (maxSize)   params.max_size  = +maxSize;
      if (startDate) params.start_date = startDate;
      if (endDate)   params.end_date   = endDate;
      // extra tag params
      tags.forEach(([k, v]) => { params[k] = v; });

      const res = await search(params);
      setResults(res.data);
    } catch (ex) {
      setError(ex.response?.data?.detail || "Search failed");
    } finally { setLoading(false); }
  };

  const clearAll = () => {
    setBucket(""); setPrefix(""); setMinSize(""); setMaxSize("");
    setStartDate(""); setEndDate(""); setTags([]); setResults(null);
  };

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-white flex items-center gap-2">
        <Search className="w-6 h-6 text-blue-400" /> Search Objects
      </h1>

      {/* Filters */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl p-5 space-y-5">
        <form onSubmit={handleSearch} className="space-y-5">
          {/* Row 1: bucket + prefix */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="text-xs text-gray-400 block mb-1">Bucket (optional)</label>
              <input value={bucket} onChange={e=>setBucket(e.target.value)} placeholder="all buckets"
                className="w-full bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-blue-500"/>
            </div>
            <div>
              <label className="text-xs text-gray-400 block mb-1">Key Prefix</label>
              <input value={prefix} onChange={e=>setPrefix(e.target.value)} placeholder="reports/"
                className="w-full bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-blue-500"/>
            </div>
          </div>

          {/* Row 2: size range */}
          <div>
            <label className="text-xs text-gray-400 flex items-center gap-1 mb-2">
              <HardDrive className="w-3 h-3"/> Size Range (bytes)
            </label>
            <div className="flex gap-3 items-center">
              <input type="number" value={minSize} onChange={e=>setMinSize(e.target.value)} placeholder="Min"
                className="flex-1 bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-blue-500"/>
              <span className="text-gray-600 text-sm">—</span>
              <input type="number" value={maxSize} onChange={e=>setMaxSize(e.target.value)} placeholder="Max"
                className="flex-1 bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-blue-500"/>
            </div>
          </div>

          {/* Row 3: date range */}
          <div>
            <label className="text-xs text-gray-400 flex items-center gap-1 mb-2">
              <Calendar className="w-3 h-3"/> Date Range (ISO-8601)
            </label>
            <div className="flex gap-3 items-center">
              <input type="datetime-local" value={startDate} onChange={e=>setStartDate(e.target.value)}
                className="flex-1 bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-blue-500"/>
              <span className="text-gray-600 text-sm">—</span>
              <input type="datetime-local" value={endDate} onChange={e=>setEndDate(e.target.value)}
                className="flex-1 bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-blue-500"/>
            </div>
          </div>

          {/* Row 4: tag filters */}
          <div>
            <label className="text-xs text-gray-400 flex items-center gap-1 mb-2">
              <Tag className="w-3 h-3"/> Tag Filters
            </label>
            <div className="flex gap-2 items-center">
              <input value={tagKey} onChange={e=>setTagKey(e.target.value)} placeholder="key"
                className="w-28 bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-blue-500"/>
              <span className="text-gray-600">=</span>
              <input value={tagVal} onChange={e=>setTagVal(e.target.value)} placeholder="value"
                className="w-28 bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-blue-500"
                onKeyDown={e => e.key === "Enter" && (e.preventDefault(), addTag())}/>
              <button type="button" onClick={addTag}
                className="bg-gray-700 hover:bg-gray-600 text-white px-3 py-2 rounded-lg text-sm">
                + Add
              </button>
            </div>
            {tags.length > 0 && (
              <div className="flex gap-2 flex-wrap mt-2">
                {tags.map(([k, v], i) => (
                  <Tag_ key={i} k={k} v={v} onRemove={() => removeTag(i)} />
                ))}
              </div>
            )}
          </div>

          {/* Buttons */}
          <div className="flex gap-3">
            <button type="submit" disabled={loading}
              className="flex items-center gap-2 bg-blue-600 hover:bg-blue-500 disabled:opacity-50 text-white font-semibold text-sm px-5 py-2 rounded-lg transition">
              <Search className="w-4 h-4" /> {loading ? "Searching…" : "Search"}
            </button>
            <button type="button" onClick={clearAll}
              className="text-gray-400 hover:text-white text-sm px-4 py-2 rounded-lg bg-gray-800 hover:bg-gray-700 transition">
              Clear
            </button>
          </div>
        </form>
      </div>

      {/* Error */}
      {error && <p className="text-red-400 bg-red-900/20 border border-red-800 rounded-lg px-4 py-3 text-sm">{error}</p>}

      {/* Results */}
      {results && (
        <div className="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden">
          <div className="px-5 py-3 border-b border-gray-800 flex items-center justify-between">
            <span className="font-semibold text-white">Results</span>
            <span className="text-xs text-gray-500">
              {results.returned} / {results.total} shown
              {results.total > results.limit && ` · showing first ${results.limit}`}
            </span>
          </div>
          {results.total === 0 ? (
            <div className="p-8 text-center text-gray-500">No objects matched your filters.</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-xs text-gray-500 border-b border-gray-800">
                    <th className="px-4 py-2">Bucket</th>
                    <th className="px-4 py-2">Key</th>
                    <th className="px-4 py-2">Size</th>
                    <th className="px-4 py-2">Chunks</th>
                    <th className="px-4 py-2">Encoding</th>
                    <th className="px-4 py-2">Created</th>
                    <th className="px-4 py-2">Tags</th>
                  </tr>
                </thead>
                <tbody>
                  {results.results.map((obj, i) => (
                    <tr key={i} className="border-b border-gray-800/50 hover:bg-gray-800/30 transition">
                      <td className="px-4 py-2 text-blue-400">{obj.bucket}</td>
                      <td className="px-4 py-2 font-mono text-xs text-white">{obj.key}</td>
                      <td className="px-4 py-2 text-gray-400">{fmt(obj.size)}</td>
                      <td className="px-4 py-2 text-gray-400">{obj.chunk_count}</td>
                      <td className="px-4 py-2">
                        <span className={`text-xs px-2 py-0.5 rounded-full ${obj.encoding === "erasure" ? "bg-purple-900/40 text-purple-300" : "bg-blue-900/40 text-blue-300"}`}>
                          {obj.encoding}
                        </span>
                      </td>
                      <td className="px-4 py-2 text-gray-500 text-xs">
                        {obj.creation_date ? new Date(obj.creation_date).toLocaleString() : "—"}
                      </td>
                      <td className="px-4 py-2">
                        <div className="flex gap-1 flex-wrap">
                          {Object.entries(obj.tags || {}).map(([k, v]) => (
                            <Tag_ key={k} k={k} v={v} />
                          ))}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
