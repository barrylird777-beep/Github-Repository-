import dns from "node:dns/promises";
import http from "node:http";
import https from "node:https";
import { isIP } from "node:net";

function ipv4Number(address) {
  const parts = address.split(".");
  if (parts.length !== 4 || parts.some(part => !/^\d{1,3}$/.test(part) || Number(part) > 255)) return null;
  return parts.reduce((value, part) => (value * 256 + Number(part)) >>> 0, 0);
}

function inCidr(value, base, bits) {
  const shift = 32 - bits;
  return (value >>> shift) === (base >>> shift);
}

function ipv4Blocked(address) {
  const value = ipv4Number(address);
  if (value === null) return true;
  const blocked = [
    ["0.0.0.0", 8], ["10.0.0.0", 8], ["100.64.0.0", 10], ["127.0.0.0", 8],
    ["169.254.0.0", 16], ["172.16.0.0", 12], ["192.0.0.0", 24], ["192.0.2.0", 24],
    ["192.88.99.0", 24], ["192.168.0.0", 16], ["198.18.0.0", 15], ["198.51.100.0", 24],
    ["203.0.113.0", 24], ["224.0.0.0", 4], ["240.0.0.0", 4]
  ];
  return blocked.some(([base, bits]) => inCidr(value, ipv4Number(base), bits));
}

function ipv4MappedFromIpv6(address) {
  const s = address.toLowerCase().split("%")[0];
  if (!s.startsWith("::ffff:")) return null;
  const tail = s.slice(7);
  if (tail.includes(".")) return tail;
  const groups = tail.split(":");
  if (groups.length !== 2 || groups.some(group => !/^[0-9a-f]{1,4}$/.test(group))) return null;
  const n = (parseInt(groups[0], 16) * 65536 + parseInt(groups[1], 16)) >>> 0;
  return [n >>> 24, (n >>> 16) & 255, (n >>> 8) & 255, n & 255].join(".");
}

export function isBlockedAddress(address) {
  const family = isIP(String(address).split("%")[0]);
  if (family === 4) return ipv4Blocked(String(address).split("%")[0]);
  if (family !== 6) return true;

  const s = String(address).toLowerCase().split("%")[0];
  const mapped = ipv4MappedFromIpv6(s);
  if (mapped) return ipv4Blocked(mapped);

  // Only globally-routable IPv6 unicast space is accepted.
  const first = parseInt(s.split(":").find(Boolean) || "0", 16);
  if (first < 0x2000 || first > 0x3fff) return true;
  const groups = s.split(":");
  const second = parseInt(groups[1] || "0", 16);
  if (s.startsWith("2001:db8:") || s === "2001:db8" ||
      (s.startsWith("2001:") && second <= 0x01ff) || s.startsWith("2002:")) return true;
  return false;
}

export async function resolveSafeUrl(raw, { lookup = dns.lookup } = {}) {
  let url;
  try { url = new URL(raw); } catch { throw new Error("invalid URL"); }
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password) {
    throw new Error("invalid URL");
  }
  if (!url.hostname || url.hostname.endsWith(".")) {
    // Trailing-dot hostnames are valid, but normalizing here keeps DNS and request
    // hostnames identical for the pinned lookup below.
    url.hostname = url.hostname.replace(/\.+$/, "");
  }
  if (!url.hostname) throw new Error("invalid URL");

  let addresses;
  try { addresses = await lookup(url.hostname, { all: true, verbatim: true }); }
  catch { throw new Error("destination DNS lookup failed"); }
  if (!Array.isArray(addresses) || addresses.length === 0 ||
      addresses.some(record => isBlockedAddress(record.address))) {
    throw new Error("private or non-public destination blocked");
  }
  return { url, addresses };
}

function requestPinned(url, addresses, maxBytes) {
  return new Promise((resolve, reject) => {
    const transport = url.protocol === "https:" ? https : http;
    let settled = false;
    const fail = error => {
      if (settled) return;
      settled = true;
      reject(error);
    };
    const request = transport.request(url, {
      method: "GET",
      headers: { "user-agent": "OpportunityEngine-Extractor/1.0", accept: "text/html,text/plain;q=0.9,*/*;q=0.1" },
      lookup: (_hostname, options, callback) => {
        const candidates = addresses.filter(record => !options.family || record.family === options.family);
        if (!candidates.length) return callback(new Error("no vetted address for destination"), "", 0);
        if (options.all) return callback(null, candidates.map(record => ({ address: record.address, family: record.family })));
        callback(null, candidates[0].address, candidates[0].family);
      }
    }, response => {
      const status = Number(response.statusCode || 0);
      if (status >= 300 && status < 400) {
        response.resume();
        fail(new Error("redirects disabled"));
        return;
      }
      if (status < 200 || status >= 300) {
        response.resume();
        fail(new Error("upstream HTTP " + status));
        return;
      }
      const chunks = [];
      let bytes = 0;
      response.on("data", chunk => {
        bytes += chunk.length;
        if (bytes > maxBytes) {
          request.destroy(new Error("response too large"));
          return;
        }
        chunks.push(chunk);
      });
      response.on("end", () => {
        if (settled) return;
        settled = true;
        resolve({ status, body: Buffer.concat(chunks) });
      });
      response.on("error", fail);
    });
    request.setTimeout(10000, () => request.destroy(new Error("upstream timeout")));
    request.on("error", fail);
    request.end();
  });
}

const strip = html => html.replace(/<script[\s\S]*?<\/script>/gi, " ")
  .replace(/<style[\s\S]*?<\/style>/gi, " ")
  .replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();

export async function extract(raw, { maxBytes = 2_000_000, lookup = dns.lookup } = {}) {
  const { url, addresses } = await resolveSafeUrl(raw, { lookup });
  const response = await requestPinned(url, addresses, maxBytes);
  const html = new TextDecoder().decode(response.body);
  return {
    url: url.href,
    status: response.status,
    title: (html.match(/<title[^>]*>([\s\S]*?)<\/title>/i)?.[1] || "").trim().slice(0, 500),
    text: strip(html).slice(0, 200_000),
    bytes: response.body.byteLength,
    extractedAt: new Date().toISOString()
  };
}
