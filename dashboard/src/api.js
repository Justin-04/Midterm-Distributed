/**
 * api.js — Axios client wired to the API Gateway.
 * Reads VITE_API_URL from .env (defaults to http://localhost:9100).
 * Automatically attaches the JWT token stored in localStorage.
 */
import axios from "axios";

const BASE = import.meta.env.VITE_API_URL || "http://localhost:9100";

const api = axios.create({ baseURL: BASE });

// Attach token on every request
api.interceptors.request.use((cfg) => {
  const token = localStorage.getItem("jwt_token");
  if (token) cfg.headers.Authorization = `Bearer ${token}`;
  return cfg;
});

// ── Auth ─────────────────────────────────────────────────────────────────────
export const login = (username, password) => {
  const form = new URLSearchParams({ username, password });
  return api.post("/auth/token", form, {
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
  });
};

export const getMe = () => api.get("/auth/me");

// ── Buckets ───────────────────────────────────────────────────────────────────
export const listBuckets   = ()           => api.get("/buckets");
export const getBucket     = (b)          => api.get(`/buckets/${b}`);
export const createBucket  = (b, cfg)     => api.put(`/buckets/${b}`, cfg);
export const deleteBucket  = (b, force)   => api.delete(`/buckets/${b}${force ? "?force=true" : ""}`);

// ── Objects ───────────────────────────────────────────────────────────────────
export const listObjects   = (b, prefix)  => api.get(`/buckets/${b}/objects${prefix ? `?prefix=${prefix}` : ""}`);
export const getObjectMeta = (b, k)       => api.get(`/buckets/${b}/objects/${k}`);
export const deleteObject  = (b, k)       => api.delete(`/buckets/${b}/${k}`);

export const uploadObject = (bucket, key, file, tags, onProgress) => {
  const form = new FormData();
  form.append("file", file);
  return api.put(`/buckets/${bucket}/${key}`, form, {
    headers: {
      "Content-Type": "multipart/form-data",
      ...(tags ? { Tags: tags } : {}),
    },
    onUploadProgress: (e) => {
      if (onProgress) onProgress(Math.round((e.loaded * 100) / e.total));
    },
  });
};

export const downloadObject = (bucket, key) =>
  api.get(`/buckets/${bucket}/${key}`, { responseType: "blob" });

export const downloadRange = (bucket, key, start, end) =>
  api.get(`/buckets/${bucket}/${key}`, {
    responseType: "blob",
    headers: { Range: `bytes=${start}-${end}` },
  });

// ── Search ────────────────────────────────────────────────────────────────────
export const search = (params) => api.get("/search", { params });
export const searchAdvanced = (params) => api.get("/search/advanced", { params });

// ── Cluster ───────────────────────────────────────────────────────────────────
export const clusterStatus = () => api.get("/cluster/status");
export const ringStats     = () => api.get("/ring/stats");
