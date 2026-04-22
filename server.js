import fs from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';
import { fileURLToPath } from 'node:url';

import express from 'express';
import session from 'express-session';
import geoip from 'geoip-lite';
import axios from 'axios';
import cors from 'cors';
import helmet from 'helmet';
import rateLimit from 'express-rate-limit';
import nodemailer from 'nodemailer';

// Load .env if present
try {
  const { default: dotenv } = await import('dotenv');
  dotenv.config();
} catch { /* dotenv optional */ }

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Railway'de /data volume kullan, yoksa local ..
const DATA_DIR = process.env.RAILWAY_VOLUME_MOUNT_PATH 
  ? process.env.RAILWAY_VOLUME_MOUNT_PATH 
  : path.resolve(__dirname, '..');
let TXT_PATH = path.join(DATA_DIR, 'dcıdsorgudata.txt');
let SQL_PATHS = [
  path.join(DATA_DIR, 'discord data.sql'),
  path.join(DATA_DIR, 'idsorgu(1).sql'),
  path.join(DATA_DIR, '840k.sql')
];

function detectDataSources() {
  let txtPath = TXT_PATH;
  let sqlPaths = SQL_PATHS;

  try {
    const entries = fs.readdirSync(DATA_DIR, { withFileTypes: true });
    const files = entries.filter(e => e.isFile()).map(e => e.name);

    const sqlFiles = files.filter(n => n.toLowerCase().endsWith('.sql'))
      .map(n => path.join(DATA_DIR, n));
    if (sqlFiles.length > 0) sqlPaths = sqlFiles;

    if (!fs.existsSync(txtPath)) {
      const txtFiles = files.filter(n => n.toLowerCase().endsWith('.txt'))
        .map(n => path.join(DATA_DIR, n));
      if (txtFiles.length > 0) txtPath = txtFiles[0];
    }
  } catch { /* ignore */ }

  TXT_PATH = txtPath;
  SQL_PATHS = sqlPaths;
  return { txtPath, sqlPaths };
}

// 🌐 Railway'de SQL dosyalarını otomatik indir
async function downloadSQLFiles() {
  if (!process.env.RAILWAY_ENVIRONMENT) return; // Sadece Railway'de çalıştır
  
  const sqlUrls = [
    { name: 'za.sql', url: 'https://drive.google.com/uc?export=download&id=12GAV9hjm1JwqJYejeFGatqud-88Vsace' },
    { name: 'zagros.sql', url: 'https://drive.google.com/uc?export=download&id=1SUoLWqm-SsbL6tDgdaP-Tc68v6B72vuZ' },
    { name: 'zagrs.sql', url: 'https://drive.google.com/uc?export=download&id=1KmjL89fGLCaeeQv4soJ2SnI7DaZS8qjA' }
  ];
  
  const txtUrls = [
    { name: 'data.txt', url: 'https://drive.google.com/uc?export=download&id=1KltBo15k2VkswKM8flAKPZYij1wbKcWZ' }
  ];
  
  for (const { name, url } of sqlUrls) {
    const filePath = path.join(DATA_DIR, name);
    if (fs.existsSync(filePath)) {
      console.log(`[Download] ${name} zaten var, atlanıyor`);
      continue;
    }
    
    try {
      console.log(`[Download] ${name} indiriliyor...`);
      const response = await axios.get(url, { 
        responseType: 'stream',
        timeout: 300000, // 5 dakika
        maxRedirects: 5
      });
      
      const writer = fs.createWriteStream(filePath);
      response.data.pipe(writer);
      
      await new Promise((resolve, reject) => {
        writer.on('finish', resolve);
        writer.on('error', reject);
      });
      
      console.log(`[Download] ${name} tamamlandı`);
    } catch (err) {
      console.error(`[Download] ${name} hata:`, err.message);
    }
  }
  
  // TXT dosyalarını da indir
  for (const { name, url } of txtUrls) {
    const filePath = path.join(DATA_DIR, name);
    if (fs.existsSync(filePath)) {
      console.log(`[Download] ${name} zaten var, atlanıyor`);
      continue;
    }
    
    try {
      console.log(`[Download] ${name} indiriliyor...`);
      const response = await axios.get(url, { 
        responseType: 'stream',
        timeout: 300000,
        maxRedirects: 5
      });
      
      const writer = fs.createWriteStream(filePath);
      response.data.pipe(writer);
      
      await new Promise((resolve, reject) => {
        writer.on('finish', resolve);
        writer.on('error', reject);
      });
      
      console.log(`[Download] ${name} tamamlandı`);
    } catch (err) {
      console.error(`[Download] ${name} hata:`, err.message);
    }
  }
  
  // SQL_PATHS'i güncelle
  detectDataSources();
}

// Railway'de dosyaları indir - AUTO DOWNLOAD KAPALI (manuel tetiklenecek)
// if (process.env.RAILWAY_ENVIRONMENT) {
//   await downloadSQLFiles();
// }

detectDataSources();

const APP_PORT = process.env.PORT ? Number(process.env.PORT) : 5178;
const APP_HOST = process.env.HOST ?? (process.env.RAILWAY_ENVIRONMENT ? '0.0.0.0' : '127.0.0.1');
const SITE_PASSWORD = process.env.ZAGROS_PASSWORD ?? '12331';
const FINDCORD_API_KEY = '1fb785c3eb8069ba341836e0b25dabb4b20e439b4bce300123da1f791f12a3ea';

// 🛡️ ZAGROS SUNUCU ÜYELİK KONTROLÜ
const ZAGROS_GUILD_ID = '852952555869044808';
const DISCORD_BOT_TOKEN = process.env.DISCORD_BOT_TOKEN || ''; // Discord bot token (opsiyonel)

// ⚡ PERFORMANS AYARLARI
const MAX_SEARCH_TIME = 8000;      // 8 saniye (normal aramalar)
const GUILD_SEARCH_TIME = 25000;   // 25 saniye (sunucu aramaları)
const MAX_RESULTS = 100;
const STREAM_BATCH = 1000;
const API_TIMEOUT = 5000;          // API çağrıları 5 sn

const FINDCORD_CACHE_TTL_MS = 5 * 60 * 1000;
const FINDCORD_NEG_TTL_MS = 60 * 1000;
const findCordCache = new Map();
const FINDCORD_RATE_LIMIT_COOLDOWN_MS = 5 * 60 * 1000; // 5 dakika cooldown
let findCordRateLimitedUntil = 0;

// Guild list cache
let guildsCache = null;
let guildsCacheTime = 0;
const CACHE_TTL = 5 * 60 * 1000; // 5 dakika cache
const GUILDS_MAX_TIME = 15000; // 15 saniye max

// 🔍 EMAIL OSINT - IntelX tarzı derinlemesine araştırma
async function performEmailOSINT(email) {
  const results = {
    email: email,
    timestamp: new Date().toISOString(),
    breaches: [],
    reputation: null,
    sources: [],
    summary: {
      total_breaches: 0,
      sensitive_breaches: 0,
      data_types_exposed: [],
      risk_level: 'unknown',
      first_breach: null,
      last_breach: null
    }
  };

  // 1. HaveIBeenPwned - Breach check
  try {
    const hibpResponse = await axios.get(
      `https://haveibeenpwned.com/api/v3/breachedaccount/${encodeURIComponent(email)}`,
      {
        headers: {
          'User-Agent': 'Zagros-OSINT-Tool',
          'Accept': 'application/json'
        },
        timeout: 8000
      }
    );

    results.breaches = hibpResponse.data.map(b => ({
      source: 'HaveIBeenPwned',
      site: b.Name,
      domain: b.Domain || b.Name.toLowerCase().replace(/\s+/g, ''),
      breach_date: b.BreachDate,
      added_date: b.AddedDate,
      description: b.Description,
      data_classes: b.DataClasses || [],
      is_verified: b.IsVerified,
      is_sensitive: b.IsSensitive,
      is_spam_list: b.IsSpamList || false,
      is_retired: b.IsRetired || false,
      pwn_count: b.PwnCount || 0,
      logo_path: b.LogoPath || null
    }));

    results.sources.push('HaveIBeenPwned');
  } catch (error) {
    if (error.response?.status === 404) {
      results.breaches = []; // Temiz, breach yok
    } else {
      console.log('[EmailOSINT] HIBP Hata:', error.message);
    }
  }

  // 2. EmailRep.io - Email reputation
  try {
    const emailRepRes = await axios.get(
      `https://emailrep.io/${encodeURIComponent(email)}`,
      {
        headers: {
          'User-Agent': 'Zagros-OSINT-Tool',
          'Key': 'public' // Ücretsiz public key
        },
        timeout: 5000
      }
    );

    const rep = emailRepRes.data;
    results.reputation = {
      source: 'EmailRep.io',
      reputation: rep.reputation || 'unknown',
      suspicious: rep.suspicious || false,
      references: rep.references || 0,
      blacklisted: rep.details?.blacklisted || false,
      malicious_activity: rep.details?.malicious_activity || false,
      malicious_activity_recent: rep.details?.malicious_activity_recent || false,
      credentials_leaked: rep.details?.credentials_leaked || false,
      credentials_leaked_recent: rep.details?.credentials_leaked_recent || false,
      data_breach: rep.details?.data_breach || false,
      first_seen: rep.details?.first_seen || null,
      last_seen: rep.details?.last_seen || null,
      domain_exists: rep.details?.domain_exists || false,
      domain_reputation: rep.details?.domain_reputation || 'unknown',
      new_domain: rep.details?.new_domain || false,
      days_since_domain_creation: rep.details?.days_since_domain_creation || null,
      spam: rep.details?.spam || false,
      free_provider: rep.details?.free_provider || false,
      disposable: rep.details?.disposable || false,
      deliverable: rep.details?.deliverable || false,
      valid_mx: rep.details?.valid_mx || false,
      spoofable: rep.details?.spoofable || false,
      spf_strict: rep.details?.spf_strict || false,
      dmarc_enforced: rep.details?.dmarc_enforced || false
    };

    results.sources.push('EmailRep.io');
  } catch (error) {
    console.log('[EmailOSINT] EmailRep Hata:', error.message);
  }

  // 3. Özet analiz
  if (results.breaches.length > 0) {
    results.summary.total_breaches = results.breaches.length;
    results.summary.sensitive_breaches = results.breaches.filter(b => b.is_sensitive).length;

    // Tüm exposed data tiplerini topla
    const allDataTypes = new Set();
    results.breaches.forEach(b => {
      if (b.data_classes) {
        b.data_classes.forEach(dc => allDataTypes.add(dc));
      }
    });
    results.summary.data_types_exposed = Array.from(allDataTypes);

    // Tarih analizi
    const dates = results.breaches
      .map(b => new Date(b.breach_date))
      .filter(d => !isNaN(d))
      .sort((a, b) => a - b);

    if (dates.length > 0) {
      results.summary.first_breach = dates[0].toISOString().split('T')[0];
      results.summary.last_breach = dates[dates.length - 1].toISOString().split('T')[0];
    }

    // Risk seviyesi hesapla
    let riskScore = 0;
    riskScore += results.breaches.length * 10;
    riskScore += results.summary.sensitive_breaches * 20;
    riskScore += results.summary.data_types_exposed.includes('Passwords') ? 30 : 0;
    riskScore += results.summary.data_types_exposed.includes('Credit card numbers') ? 40 : 0;
    riskScore += results.reputation?.suspicious ? 15 : 0;
    riskScore += results.reputation?.blacklisted ? 25 : 0;

    if (riskScore >= 80) results.summary.risk_level = 'critical';
    else if (riskScore >= 50) results.summary.risk_level = 'high';
    else if (riskScore >= 20) results.summary.risk_level = 'medium';
    else results.summary.risk_level = 'low';
  } else {
    results.summary.risk_level = results.reputation?.suspicious ? 'medium' : 'clean';
  }

  return results;
}

// IP Geolocation - Detaylı konum bilgisi
async function getIpGeolocation(ip) {
  try {
    // IP format kontrolü
    if (!ip || !/^(\d{1,3}\.){3}\d{1,3}$/.test(ip)) return null;
    
    const response = await axios.get(`http://ip-api.com/json/${ip}?fields=status,message,continent,continentCode,country,countryCode,region,regionName,city,district,zip,lat,lon,timezone,offset,currency,isp,org,as,asname,reverse,mobile,proxy,hosting,query`, {
      timeout: 5000
    });
    
    if (response.data.status === 'success') {
      return {
        ip: response.data.query,
        continent: response.data.continent,
        country: response.data.country,
        countryCode: response.data.countryCode,
        region: response.data.regionName,
        city: response.data.city,
        district: response.data.district, // Mahalle/semt
        zip: response.data.zip,
        lat: response.data.lat,
        lon: response.data.lon,
        timezone: response.data.timezone,
        isp: response.data.isp,
        org: response.data.org,
        mobile: response.data.mobile,
        proxy: response.data.proxy,
        hosting: response.data.hosting
      };
    }
    return null;
  } catch (error) {
    console.log(`[IP-API] Hata ${ip}:`, error.message);
    return null;
  }
}

// Phone validasyon ve bilgi
function validatePhone(phone) {
  // Temizle
  const clean = phone.replace(/\D/g, '');
  const result = { valid: false, original: phone, cleaned: clean, country: null, carrier: null, type: null };
  
  // E.164 format kontrolü (temel)
  if (clean.length < 7 || clean.length > 15) return result;
  result.valid = true;
  
  // Ülke kodu tahmini
  const countryCodes = {
    '1': 'US/CA', '7': 'RU/KZ', '33': 'FR', '49': 'DE', '90': 'TR',
    '44': 'GB', '39': 'IT', '34': 'ES', '91': 'IN', '86': 'CN',
    '81': 'JP', '82': 'KR', '61': 'AU', '55': 'BR', '52': 'MX'
  };
  
  for (const [code, country] of Object.entries(countryCodes)) {
    if (clean.startsWith(code)) {
      result.country = country;
      result.countryCode = code;
      break;
    }
  }
  
  // Türkiye için operatör tahmini
  if (result.country === 'TR' || clean.startsWith('90')) {
    const prefix = clean.substring(2, 5);
    const operators = {
      '530': 'Turkcell', '531': 'Turkcell', '532': 'Turkcell', '533': 'Turkcell', '534': 'Turkcell',
      '535': 'Turkcell', '536': 'Turkcell', '537': 'Turkcell', '538': 'Turkcell', '539': 'Turkcell',
      '540': 'Vodafone', '541': 'Vodafone', '542': 'Vodafone', '543': 'Vodafone', '544': 'Vodafone',
      '545': 'Vodafone', '546': 'Vodafone', '547': 'Vodafone', '548': 'Vodafone', '549': 'Vodafone',
      '505': 'Turk Telekom', '506': 'Turk Telekom', '507': 'Turk Telekom', '551': 'Turk Telekom',
      '552': 'Turk Telekom', '553': 'Turk Telekom', '554': 'Turk Telekom', '555': 'Turk Telekom'
    };
    result.carrier = operators[prefix] || 'Bilinmiyor';
    
    // Tip belirleme
    if (['530', '531', '532', '533', '534', '535', '536', '537', '538', '539'].includes(prefix)) {
      result.type = 'GSM';
    } else if (['540', '541', '542', '543', '544', '545', '546', '547', '548', '549'].includes(prefix)) {
      result.type = 'GSM';
    } else if (['505', '506', '507', '551', '552', '553', '554', '555'].includes(prefix)) {
      result.type = 'GSM';
    }
  }
  
  return result;
}

// Domain lookup (WHOIS + DNS)
async function lookupDomain(domain) {
  try {
    // WHOIS bilgisi (whoisjson.com - free tier)
    const whoisRes = await axios.get(`https://whoisjson.com/api/v1/whois?domain=${encodeURIComponent(domain)}`, {
      timeout: 8000
    }).catch(() => null);
    
    // DNS records
    const dnsInfo = {
      a: [],
      mx: [],
      txt: [],
      ns: []
    };
    
    // Basic DNS lookup simulation (gerçek DNS lookup için dns modülü gerekir)
    // Şimdilik basic bilgi
    return {
      domain: domain,
      whois: whoisRes?.data || null,
      dns: dnsInfo,
      available: whoisRes?.data?.available || false,
      created: whoisRes?.data?.created || null,
      expires: whoisRes?.data?.expires || null,
      registrar: whoisRes?.data?.registrar?.name || null
    };
  } catch (error) {
    return { domain, error: error.message };
  }
}

// Username OSINT - çoklu platform
async function searchUsername(username) {
  const results = [];
  
  // GitHub
  try {
    const ghRes = await axios.get(`https://api.github.com/users/${encodeURIComponent(username)}`, {
      headers: { 'Accept': 'application/vnd.github.v3+json' },
      timeout: 5000
    });
    results.push({
      platform: 'GitHub',
      available: true,
      url: ghRes.data.html_url,
      avatar: ghRes.data.avatar_url,
      name: ghRes.data.name,
      bio: ghRes.data.bio,
      location: ghRes.data.location,
      company: ghRes.data.company,
      blog: ghRes.data.blog,
      public_repos: ghRes.data.public_repos,
      followers: ghRes.data.followers,
      following: ghRes.data.following,
      created_at: ghRes.data.created_at
    });
  } catch { 
    results.push({ platform: 'GitHub', available: false });
  }
  
  // Twitter/X (public profile check - basic)
  try {
    // Twitter API artık çok kısıtlı, basic URL check yapıyoruz
    const twRes = await axios.head(`https://twitter.com/${encodeURIComponent(username)}`, {
      timeout: 5000,
      validateStatus: () => true
    });
    if (twRes.status === 200) {
      results.push({
        platform: 'Twitter/X',
        available: true,
        url: `https://twitter.com/${username}`,
        note: 'Profil var (detay için API gerekli)'
      });
    } else {
      results.push({ platform: 'Twitter/X', available: false });
    }
  } catch {
    results.push({ platform: 'Twitter/X', available: false });
  }
  
  // Instagram (basic check)
  try {
    const igRes = await axios.head(`https://instagram.com/${encodeURIComponent(username)}`, {
      timeout: 5000,
      validateStatus: () => true
    });
    if (igRes.status === 200) {
      results.push({
        platform: 'Instagram',
        available: true,
        url: `https://instagram.com/${username}`,
        note: 'Profil var (detay için scraping gerekli)'
      });
    } else {
      results.push({ platform: 'Instagram', available: false });
    }
  } catch {
    results.push({ platform: 'Instagram', available: false });
  }
  
  // Reddit
  try {
    const rdRes = await axios.get(`https://www.reddit.com/user/${encodeURIComponent(username)}/about.json`, {
      timeout: 5000,
      headers: { 'User-Agent': 'Zagros-OSINT/1.0' }
    });
    if (rdRes.data?.data) {
      results.push({
        platform: 'Reddit',
        available: true,
        url: `https://reddit.com/user/${username}`,
        karma: rdRes.data.data.total_karma,
        created: new Date(rdRes.data.data.created_utc * 1000).toISOString()
      });
    }
  } catch {
    results.push({ platform: 'Reddit', available: false });
  }
  
  return results;
}

// Spotify'da email ara (public API yok, breach verisinden veya basic check)
async function searchSpotifyByEmail(email) {
  // Spotify public API'de email arama yok
  // Ancak breach verilerinden veya başka kaynaklardan çıkarım yapılabilir
  // Şimdilik placeholder - gerçek implementation için:
  // - Spotify breach verisi gerekiyor
  // - Veya Spotify web scraping (rate limitli)
  return [];
}

// Reddit'te email ara (varsa)
async function searchRedditByEmail(email) {
  try {
    // Reddit'te email arama doğrudan yok
    // Ancak bazı durumlarda username pattern'i çıkarılabilir
    return [];
  } catch {
    return [];
  }
}

// Platform bazlı email arama - Intelx tarzı
async function searchPlatformsByEmail(email) {
  const platforms = [];
  const username = email.split('@')[0]; // email'den username tahmini
  
  // Spotify pattern check (örnek: spotify'da username = email prefix olabilir)
  // Gerçek Spotify API'si olmadığından simülasyon yapıyoruz
  // Gerçek implementation için Spotify hesap recovery endpoint'i kullanılabilir
  
  // LinkedIn pattern (public search)
  try {
    const lnCheck = await axios.head(`https://www.linkedin.com/in/${encodeURIComponent(username)}`, {
      timeout: 5000,
      validateStatus: () => true
    });
    if (lnCheck.status === 200) {
      platforms.push({
        platform: 'LinkedIn',
        found: true,
        username: username,
        url: `https://linkedin.com/in/${username}`,
        note: 'Profil var (detay için API gerekli)',
        confidence: 'medium'
      });
    }
  } catch { /* ignore */ }
  
  // Pinterest check
  try {
    const ptCheck = await axios.head(`https://pinterest.com/${encodeURIComponent(username)}`, {
      timeout: 5000,
      validateStatus: () => true
    });
    if (ptCheck.status === 200) {
      platforms.push({
        platform: 'Pinterest',
        found: true,
        username: username,
        url: `https://pinterest.com/${username}`,
        confidence: 'medium'
      });
    }
  } catch { /* ignore */ }
  
  // Tumblr check
  try {
    const tmCheck = await axios.head(`https://${encodeURIComponent(username)}.tumblr.com`, {
      timeout: 5000,
      validateStatus: () => true
    });
    if (tmCheck.status === 200) {
      platforms.push({
        platform: 'Tumblr',
        found: true,
        username: username,
        url: `https://${username}.tumblr.com`,
        confidence: 'medium'
      });
    }
  } catch { /* ignore */ }
  
  // Twitch check
  try {
    const twCheck = await axios.head(`https://twitch.tv/${encodeURIComponent(username)}`, {
      timeout: 5000,
      validateStatus: () => true
    });
    if (twCheck.status === 200) {
      platforms.push({
        platform: 'Twitch',
        found: true,
        username: username,
        url: `https://twitch.tv/${username}`,
        confidence: 'medium'
      });
    }
  } catch { /* ignore */ }
  
  // TikTok check
  try {
    const ttCheck = await axios.head(`https://tiktok.com/@${encodeURIComponent(username)}`, {
      timeout: 5000,
      validateStatus: () => true
    });
    if (ttCheck.status === 200) {
      platforms.push({
        platform: 'TikTok',
        found: true,
        username: username,
        url: `https://tiktok.com/@${username}`,
        confidence: 'low'
      });
    }
  } catch { /* ignore */ }
  
  // Medium check
  try {
    const mdCheck = await axios.head(`https://medium.com/@${encodeURIComponent(username)}`, {
      timeout: 5000,
      validateStatus: () => true
    });
    if (mdCheck.status === 200) {
      platforms.push({
        platform: 'Medium',
        found: true,
        username: username,
        url: `https://medium.com/@${username}`,
        confidence: 'medium'
      });
    }
  } catch { /* ignore */ }
  
  // SoundCloud check (müzik platformu - Spotify alternatifi)
  try {
    const scCheck = await axios.head(`https://soundcloud.com/${encodeURIComponent(username)}`, {
      timeout: 5000,
      validateStatus: () => true
    });
    if (scCheck.status === 200) {
      platforms.push({
        platform: 'SoundCloud',
        found: true,
        username: username,
        url: `https://soundcloud.com/${username}`,
        note: 'Müzik platformu (Spotify alternatifi)',
        confidence: 'medium'
      });
    }
  } catch { /* ignore */ }
  
  // Dev.to check (geliştirici platformu)
  try {
    const dvCheck = await axios.head(`https://dev.to/${encodeURIComponent(username)}`, {
      timeout: 5000,
      validateStatus: () => true
    });
    if (dvCheck.status === 200) {
      platforms.push({
        platform: 'Dev.to',
        found: true,
        username: username,
        url: `https://dev.to/${username}`,
        confidence: 'medium'
      });
    }
  } catch { /* ignore */ }
  
  // TryHackMe check (cybersecurity platformu)
  try {
    const thmCheck = await axios.head(`https://tryhackme.com/p/${encodeURIComponent(username)}`, {
      timeout: 5000,
      validateStatus: () => true
    });
    if (thmCheck.status === 200) {
      platforms.push({
        platform: 'TryHackMe',
        found: true,
        username: username,
        url: `https://tryhackme.com/p/${username}`,
        confidence: 'medium'
      });
    }
  } catch { /* ignore */ }
  
  return platforms;
}

// HaveIBeenPwned standalone breach check
async function checkHaveIBeenPwned(email) {
  try {
    const res = await axios.get(
      `https://haveibeenpwned.com/api/v3/breachedaccount/${encodeURIComponent(email)}`,
      {
        headers: {
          'User-Agent': 'Zagros-OSINT-Tool',
          'Accept': 'application/json'
        },
        timeout: 8000
      }
    );
    return res.data.map(b => ({
      site: b.Name,
      domain: b.Domain || b.Name.toLowerCase().replace(/\s+/g, ''),
      breach_date: b.BreachDate,
      description: b.Description,
      data_classes: b.DataClasses || [],
      is_sensitive: b.IsSensitive || false,
      pwn_count: b.PwnCount || 0
    }));
  } catch (error) {
    if (error.response?.status === 404) return []; // No breaches
    console.log('[HIBP] Hata:', error.message);
    return [];
  }
}

// Daha fazla breach kaynağı
async function checkLeakLookup(email) {
  // Leak-Lookup API (ücretsiz tier var ama API key gerekli)
  // Şimdilik placeholder
  return null;
}

async function checkEmailrep(email) {
  try {
    const res = await axios.get(`https://emailrep.io/${encodeURIComponent(email)}`, {
      timeout: 5000
    });
    return {
      reputation: res.data.reputation,
      suspicious: res.data.suspicious,
      references: res.data.references,
      details: res.data.details
    };
  } catch {
    return null;
  }
}

// Gravatar - Email'den profil bilgisi
async function getGravatarInfo(email) {
  try {
    const crypto = await import('crypto');
    const hash = crypto.createHash('md5').update(email.toLowerCase().trim()).digest('hex');
    const response = await axios.get(`https://gravatar.com/${hash}.json`, {
      timeout: 5000
    });
    const entry = response.data?.entry?.[0];
    if (!entry) return null;
    return {
      username: entry.preferredUsername,
      name: entry.displayName,
      avatar: entry.photos?.[0]?.value,
      profile_url: entry.profileUrl,
      urls: entry.urls || [],
      accounts: entry.accounts || []
    };
  } catch (error) {
    return null;
  }
}

// GitHub'da email ara
async function searchGitHubByEmail(email) {
  try {
    // GitHub API'de email arama yok, ama commit'lerde email geçmişi olabilir
    // GitHub Search API - users by email in bio/location (public)
    const searchRes = await axios.get(`https://api.github.com/search/users?q=${encodeURIComponent(email)}+in:email`, {
      headers: { 'Accept': 'application/vnd.github.v3+json' },
      timeout: 5000
    });
    const users = [];
    if (searchRes.data?.items) {
      for (const u of searchRes.data.items.slice(0, 5)) {
        // Kullanıcı detaylarını çek
        try {
          const userRes = await axios.get(`https://api.github.com/users/${u.login}`, {
            headers: { 'Accept': 'application/vnd.github.v3+json' },
            timeout: 5000
          });
          const ud = userRes.data;
          users.push({
            site: 'GitHub',
            username: ud.login,
            url: ud.html_url,
            avatar: ud.avatar_url,
            name: ud.name || null,
            bio: ud.bio || null,
            location: ud.location || null,
            company: ud.company || null,
            blog: ud.blog || null,
            public_repos: ud.public_repos,
            followers: ud.followers,
            following: ud.following,
            created_at: ud.created_at
          });
        } catch { /* skip */ }
      }
    }
    return users;
  } catch (err) {
    console.log('[GitHub] Arama hatası:', err.message);
    return [];
  }
}

// Email validasyon
function validateEmail(email) {
  const result = { valid: false, format: false, domain: null, disposable: false, free: false };
  
  // Format kontrolü
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  result.format = emailRegex.test(email);
  if (!result.format) return result;
  result.valid = true;
  
  // Domain analizi
  const domain = email.split('@')[1].toLowerCase();
  result.domain = domain;
  
  // Free email sağlayıcıları
  const freeDomains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'live.com', 'yandex.com', 'mail.ru', 'protonmail.com', 'icloud.com'];
  result.free = freeDomains.includes(domain);
  
  // Disposable email listesi (basit)
  const disposableDomains = ['tempmail.com', 'throwaway.com', 'mailinator.com', 'guerrillamail.com', '10minutemail.com', 'fakeemail.com'];
  result.disposable = disposableDomains.includes(domain);
  
  return result;
}

// FindCord API (RAW)
// Not: Uygulamanın diğer kısımları `UserInfo`, `Guilds`, `GuildName` gibi alanları bekliyor.
// Bu yüzden burada RAW response dönüyoruz.
async function getFindCordData(userId) {
  try {
    if (Date.now() < findCordRateLimitedUntil) return null;

    const cacheKey = String(userId);
    const cached = findCordCache.get(cacheKey);
    if (cached && (Date.now() - cached.time) < cached.ttl) {
      return cached.data;
    }

    const response = await axios.get(`https://app.findcord.com/api/user/${userId}`, {
      headers: {
        'Authorization': FINDCORD_API_KEY,
        'Accept': 'application/json'
      },
      timeout: API_TIMEOUT
    });
    findCordCache.set(cacheKey, { time: Date.now(), ttl: FINDCORD_CACHE_TTL_MS, data: response.data });
    return response.data;
  } catch (error) {
    const status = error.response?.status;
    const message = error.response?.data?.message || error.response?.statusText || error.message;
    console.log(`[FindCord] ✗ Hata ${userId}: ${status} - ${message}`);
    const cacheKey = String(userId);
    if (status === 404 || status === 429) {
      findCordCache.set(cacheKey, { time: Date.now(), ttl: FINDCORD_NEG_TTL_MS, data: null });
    }
    if (status === 429) {
      findCordRateLimitedUntil = Date.now() + FINDCORD_RATE_LIMIT_COOLDOWN_MS;
      console.log(`[FindCord] ⚠️ Rate limit! ${FINDCORD_RATE_LIMIT_COOLDOWN_MS/60000} dk bekleme`);
    }
    return null;
  }
}

function normalizeFindCordData(userId, data) {
  if (!data) return null;
  const ui = data.UserInfo || data.userInfo || {};
  const avatar = ui.UserdisplayAvatar || ui.avatar || data.avatar || null;
  const banner = ui.UserBanner || ui.banner || data.banner || null;

  const normalized = {
    id: userId,
    username: ui.UserName || ui.username || data.username || null,
    global_name: ui.UserGlobalName || ui.global_name || data.global_name || null,
    avatar,
    banner,
    bio: ui.UserBio || ui.bio || data.bio || null,
    pronouns: ui.UserPronouns || ui.pronouns || data.pronouns || null,
    presence: ui.Presence || data.Presence || data.presence || null,
    badges: Array.isArray(ui.UserBadge) ? ui.UserBadge : (data.badges || data.Badges || []),
    guilds: data.Guilds || data.guilds || [],
    raw: data
  };

  if (avatar) {
    normalized.avatar_url = avatar.startsWith('http')
      ? avatar
      : `https://cdn.discordapp.com/avatars/${userId}/${avatar}.png?size=256`;
  } else {
    const defaultIndex = parseInt(userId) % 5;
    normalized.avatar_url = `https://cdn.discordapp.com/embed/avatars/${defaultIndex}.png`;
  }

  if (banner) {
    normalized.banner_url = banner.startsWith('http')
      ? banner
      : `https://cdn.discordapp.com/banners/${userId}/${banner}.png?size=512`;
  }

  return normalized;
}

// �️ SUNUCU İSMİ ÇÖZÜMLEME SİSTEMİ (Bot token gerekmez!)
const GUILD_NAMES_CACHE_FILE = path.join(DATA_DIR, 'guild_names_cache.json');
let guildNamesCache = new Map();

// Cache'i yükle
try {
  if (fs.existsSync(GUILD_NAMES_CACHE_FILE)) {
    const cached = JSON.parse(fs.readFileSync(GUILD_NAMES_CACHE_FILE, 'utf8'));
    guildNamesCache = new Map(Object.entries(cached));
    console.log(`[Guild Names] ${guildNamesCache.size} cached name loaded`);
  }
} catch { /* ignore */ }

function saveGuildNamesCache() {
  try {
    const obj = Object.fromEntries(guildNamesCache);
    fs.writeFileSync(GUILD_NAMES_CACHE_FILE, JSON.stringify(obj, null, 2));
  } catch { /* ignore */ }
}

// Discord Widget API - Herkese açık, token gerekmez!
async function fetchDiscordWidget(guildId) {
  try {
    const res = await axios.get(`https://discord.com/api/v9/guilds/${guildId}/widget.json`, {
      timeout: 3000, // Daha kısa timeout
      headers: { 'Accept': 'application/json' }
    });
    if (res.data?.name) {
      return {
        name: res.data.name,
        instant_invite: res.data.instant_invite || null,
        presence_count: res.data.presence_count || 0,
        source: 'widget'
      };
    }
  } catch (e) {
    // Widget kapalı olabilir, 404 normal
  }
  return null;
}

// Disboard.org'dan sunucu ismi çek
async function fetchDisboardInfo(guildId) {
  try {
    const res = await axios.get(`https://disboard.org/search?keyword=${guildId}`, {
      timeout: 4000, // Daha kısa timeout
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
      }
    });
    const html = res.data;
    const nameMatch = html.match(new RegExp(`data-id="${guildId}"[^>]*>[^<]*<h3[^>]*>([^<]+)</h3`, 'i'));
    if (nameMatch) {
      return { name: nameMatch[1].trim(), source: 'disboard' };
    }
  } catch { /* ignore */ }
  return null;
}

// Tüm kaynaklardan sunucu ismi çözümle
async function resolveGuildName(guildId) {
  // Önce cache kontrol
  if (guildNamesCache.has(guildId)) {
    return { name: guildNamesCache.get(guildId), source: 'cache' };
  }

  // 1. Discord Widget API (en hızlı, public)
  const widget = await fetchDiscordWidget(guildId);
  if (widget?.name) {
    guildNamesCache.set(guildId, widget.name);
    saveGuildNamesCache();
    return { name: widget.name, icon: null, source: 'widget' };
  }

  // 2. Disboard.org (yedek)
  const disboard = await fetchDisboardInfo(guildId);
  if (disboard?.name) {
    guildNamesCache.set(guildId, disboard.name);
    saveGuildNamesCache();
    return { name: disboard.name, icon: null, source: 'disboard' };
  }

  return null;
}

// Toplu sunucu isim çözümleme - hızlı versiyon
async function batchResolveGuildNames(guilds) {
  const results = [];
  const batchSize = 10; // Daha fazla paralel istek

  for (let i = 0; i < guilds.length; i += batchSize) {
    const batch = guilds.slice(i, i + batchSize);
    const batchPromises = batch.map(async (guild) => {
      if (guild.name && guild.name !== 'Bilinmeyen Sunucu') {
        return { id: guild.id, name: guild.name, source: 'existing' };
      }

      const resolved = await resolveGuildName(guild.id);
      if (resolved) {
        return { id: guild.id, ...resolved };
      }

      return { id: guild.id, name: null, source: 'not_found' };
    });

    const batchResults = await Promise.allSettled(batchPromises);
    results.push(...batchResults);

    // Daha kısa bekleme
    if (i + batchSize < guilds.length) {
      await new Promise(r => setTimeout(r, 200));
    }
  }

  return results;
}

// �🎨 GELİŞMİŞ FINDCORD VERİ İŞLEME
function enrichWithFindCord(baseData, fcData) {
  if (!fcData) return baseData;
  
  return {
    ...baseData,
    // Profil
    username: baseData.username || fcData.username,
    global_name: baseData.global_name || fcData.global_name,
    avatar_hash: baseData.avatar_hash || fcData.avatar,
    avatar_url: fcData.avatar_url,
    banner_url: fcData.banner_url,
    bio: baseData.bio || fcData.bio,
    pronouns: baseData.pronouns || fcData.pronouns,
    created_at: baseData.created_at || fcData.created_at,
    
    // Rozetler
    badges: fcData.badges || [],
    
    // FindCord metadata
    findcord_enriched: true,
    findcord_guilds: fcData.guilds || []
  };
}

function maskEmail(email) {
  if (!email || typeof email !== 'string') return null;
  return email;
}

function maskIp(ip) {
  if (!ip || typeof ip !== 'string') return null;
  return ip;
}

function getIpLocation(ip) {
  if (!ip || typeof ip !== 'string') return null;
  // IPv6 veya hash ise location çıkamaz
  if (ip.includes(':') || ip.match(/^[a-f0-9]{32}$/i)) return null;
  const geo = geoip.lookup(ip);
  if (!geo) return null;
  const parts = [];
  if (geo.city) parts.push(geo.city);
  if (geo.region) parts.push(geo.region);
  if (geo.country) parts.push(geo.country);
  return parts.length > 0 ? parts.join(', ') : null;
}

function safeJsonParse(s) {
  try {
    return JSON.parse(s);
  } catch {
    return null;
  }
}

function decodeBase64Maybe(s) {
  if (!s || typeof s !== 'string') return s;
  const trimmed = s.trim();
  if (!/^[A-Za-z0-9+/=]+$/.test(trimmed)) return s;
  if (trimmed.length < 8) return s;
  try {
    const buf = Buffer.from(trimmed, 'base64');
    const decoded = buf.toString('utf8');
    if (decoded.includes('@') || decoded.includes('.') || decoded.includes(' ')) return decoded;
    return s;
  } catch {
    return s;
  }
}

function tryParseConnectionsValue(raw) {
  if (raw == null) return null;
  if (typeof raw !== 'string') return null;

  const trimmed = raw.trim();
  if (!trimmed) return null;

  if (trimmed === '[]') return [];
  if (trimmed === '{}' || trimmed === '{ }') return {};

  if (trimmed.startsWith('"') && trimmed.endsWith('"')) {
    const unquoted = trimmed.slice(1, -1);
    const unescaped = unquoted.replace(/\\"/g, '"').replace(/\\\\/g, '\\');
    const parsed = safeJsonParse(unescaped);
    return parsed;
  }

  if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
    return safeJsonParse(trimmed);
  }

  return null;
}

function extractConnectionAppsFromLine(line) {
  if (!line || typeof line !== 'string') return [];

  // Hem tırnaklı JSON hem de SQL sütun verisi olarak yakalamayı dene
  const mConnections = line.match(/(?:"connections"|'connections'|connections)\s*[:=,]\s*(\[.*?\]|\{.*?\}|'.*?'|".*?")/i);
  const raw = mConnections?.[1];
  
  // SQL'deki tek tırnaklı stringleri temizle
  let cleanRaw = raw;
  if (raw && raw.startsWith("'") && raw.endsWith("'")) {
    cleanRaw = raw.slice(1, -1).replace(/''/g, "'");
  }

  const parsed = tryParseConnectionsValue(cleanRaw);

  if (!parsed) return [];
  if (Array.isArray(parsed)) return [];
  if (typeof parsed !== 'object') return [];

  // Uygulama adı, id ve nick çıkar
  return Object.entries(parsed).map(([app, detail]) => {
    let connId = '', nick = '';
    if (typeof detail === 'string') {
      nick = detail;
    } else if (typeof detail === 'object' && detail !== null) {
      const entries = Object.entries(detail);
      if (entries.length > 0) {
        connId = String(entries[0][0]);
        nick = String(entries[0][1]);
      }
    }
    return { app, id: connId, name: nick };
  }).slice(0, 10);
}

async function searchTxtByDiscordId(discordId) {
  if (!fs.existsSync(TXT_PATH)) return [];
  const content = await fs.promises.readFile(TXT_PATH, 'utf8');
  const obj = safeJsonParse(content);
  const users = Array.isArray(obj?.users) ? obj.users : [];

  const matches = users.filter((u) => String(u?.discord_id ?? '') === String(discordId));
  return matches.map((u) => ({
    source: path.basename(TXT_PATH),
    discord_id: String(u.discord_id ?? ''),
    username: u.username ?? null,
    discriminator: u.discriminator ?? null,
    email_masked: maskEmail(u.email ?? null),
    registration_ip_masked: maskIp(u.registration_ip ?? null),
    last_ip_masked: maskIp(u.last_ip ?? null),
    created_at: u.created_at ?? null,
    last_login: u.last_login ?? null,
    subscription_type: u.subscription_type ?? null,
    is_active: u.is_active ?? null
  }));
}

function extractField(line, fieldName) {
  // "email":"value" veya "ip":"value" gibi alanları direkt regex ile çıkar
  const m = line.match(new RegExp(`"${fieldName}"\\s*:\\s*"([^"]*)"`, 'i'));
  return m ? m[1] : null;
}

function extractConnectionsFromLine(line) {
  // connections alanını bul - string veya object olarak
  const m = line.match(/"connections"\s*:\s*"/);
  if (!m) {
    // connections object olarak: "connections":{...}
    const m2 = line.match(/"connections"\s*:\s*(\{[^}]*\})/);
    if (m2) {
      const parsed = safeJsonParse(m2[1]);
      if (parsed && typeof parsed === 'object') return parseConnObj(parsed);
    }
    return [];
  }

  // connections string olarak: "connections":"{\"github\":...}"
  // SQL'de \\" olarak saklanıyor, önce temizle
  const startIdx = line.indexOf('"connections"');
  if (startIdx === -1) return [];

  // connections değerinin başlangıcını bul (ilk " işaretinden sonra)
  const valStart = line.indexOf(':', startIdx);
  if (valStart === -1) return [];

  // String mi object mi kontrol et
  let valContent = line.substring(valStart + 1).trim();

  if (valContent.startsWith('"')) {
    // String içindeki JSON: "...\"github\"..."
    // Kapanış tırnağını bul (kaçışlı olmayan)
    let endIdx = -1;
    for (let i = 1; i < valContent.length; i++) {
      if (valContent[i] === '"' && valContent[i - 1] !== '\\') {
        endIdx = i;
        break;
      }
    }
    if (endIdx === -1) return [];

    let rawConn = valContent.substring(1, endIdx);
    // Çift kaçışları temizle: \\" -> "
    rawConn = rawConn.replace(/\\\\"/g, '"').replace(/\\"/g, '"').replace(/\\\\/g, '\\');

    // "[]" ise boş
    if (rawConn === '[]' || rawConn.trim() === '') return [];

    const parsed = safeJsonParse(rawConn);
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) return parseConnObj(parsed);
    return [];
  }

  if (valContent.startsWith('{')) {
    let depth = 0;
    let endIdx = -1;
    for (let i = 0; i < valContent.length; i++) {
      if (valContent[i] === '{') depth++;
      else if (valContent[i] === '}') { depth--; if (depth === 0) { endIdx = i + 1; break; } }
    }
    if (endIdx > 0) {
      const rawConn = valContent.substring(0, endIdx);
      const parsed = safeJsonParse(rawConn);
      if (parsed && typeof parsed === 'object') return parseConnObj(parsed);
    }
  }

  return [];
}

function parseConnObj(obj) {
  return Object.entries(obj).map(([app, detail]) => {
    let connId = '', nick = '';
    if (typeof detail === 'string') nick = detail;
    else if (typeof detail === 'object' && detail !== null) {
      const entries = Object.entries(detail);
      if (entries.length > 0) { connId = String(entries[0][0]); nick = String(entries[0][1]); }
    }
    return { app, id: connId, name: nick };
  });
}

async function scanSqlFileForDiscordId(sqlPath, discordId, maxHits = 50) {
  if (!fs.existsSync(sqlPath)) return [];
  console.log(`[Tarama] Başlıyor: ${path.basename(sqlPath)}`);

  const matches = [];
  try {
    const rs = fs.createReadStream(sqlPath, { encoding: 'utf8' });
    const rl = readline.createInterface({ input: rs, crlfDelay: Infinity });

    const needle = String(discordId);

    for await (const line of rl) {
      if (!line.includes(needle)) continue;

      let email = null, ip = null, username = null, discriminator = null;
      let avatar_hash = null, bio = null, premium = null, verified = null;
      let connections_apps = [];
      let isUsersTable = false;

      // === FORMAT 1: users tablosu INSERT ===
      if (line.includes('INSERT INTO') && (line.includes('`users`') || line.includes('users'))) {
        isUsersTable = true;
        const vals = [...line.matchAll(/'([^']*)'/g)].map(m => m[1]);
        if (vals.length >= 6) {
          username = vals[2] || null;
          discriminator = vals[3] || null;
          email = vals[4] || null;
          avatar_hash = vals[5] || null;
          // registration_ip ve last_ip sondaki IP'ler
          for (let vi = vals.length - 1; vi >= 0; vi--) {
            if (vals[vi].match(/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/)) {
              if (!ip) ip = vals[vi]; // last_ip
              else { /* registration_ip de var */ }
            }
          }
        }
      }

      // === FORMAT 1b: discord_ids tablosu tuple ===
      if (!isUsersTable && line.match(/\(\s*\d+\s*,/)) {
        const tupleMatch = line.match(/\(\s*\d+\s*,\s*'(\d{10,20})'/);
        if (tupleMatch) discordId = tupleMatch[1];
        const tupleVals = [...line.matchAll(/'([^']*)'/g)].map(m => m[1]);
        // 1. değer = email (base64)
        if (!email && tupleVals.length >= 1) email = decodeBase64Maybe(tupleVals[0]);
        // 5. değer = IP (IPv4 veya IPv6 veya hash)
        if (!ip && tupleVals.length >= 5) {
          const candidate = tupleVals[4];
          if (candidate && (candidate.match(/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/) || candidate.includes(':'))) {
            ip = candidate;
          }
        }
        // Diğer IP'leri de ara
        if (!ip) {
          for (const v of tupleVals) {
            if (v.match(/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/)) { ip = v; break; }
            if (v.match(/^[0-9a-f]{1,4}(:[0-9a-f]{1,4}){2,7}$/i)) { ip = v; break; }
          }
        }
      }

      // === FORMAT 2: query_logs / response_data JSON ===
      if (!isUsersTable) {
        // Direkt regex ile alanları çıkar
        const rawEmail = extractField(line, 'email');
        if (rawEmail) email = decodeBase64Maybe(rawEmail);

        const rawIp = extractField(line, 'ip');
        if (rawIp && !rawIp.match(/^[a-f0-9]{32}$/)) ip = rawIp;

        const rawUser = extractField(line, 'username');
        if (rawUser && rawUser !== 'N/A' && rawUser !== 'N\\/A') username = rawUser;

        const rawDisc = extractField(line, 'discriminator');
        if (rawDisc && rawDisc !== 'N/A' && rawDisc !== 'N\\/A') discriminator = rawDisc;

        const rawAvatar = extractField(line, 'avatar_hash');
        if (rawAvatar && rawAvatar !== 'N/A' && rawAvatar !== 'N\\/A') avatar_hash = rawAvatar;

        const rawBio = extractField(line, 'bio');
        if (rawBio && rawBio !== 'null') bio = rawBio;

        const rawPremium = extractField(line, 'premium');
        if (rawPremium !== null) premium = rawPremium;

        const rawVerified = extractField(line, 'verified');
        if (rawVerified !== null) verified = rawVerified;

        // Connections çıkar
        connections_apps = extractConnectionsFromLine(line);

        // SQL tuple formatı: (id, 'base64email', ...) — 2. değer genelde email
        if (!email && line.includes(`(${needle},`)) {
          const tupleMatch = line.match(/\(\s*\d+\s*,\s*'([^']+)'/);
          if (tupleMatch) email = decodeBase64Maybe(tupleMatch[1]);
        }

        // SQL tuple: IP ara
        if (!ip && line.includes(`(${needle},`)) {
          const vals = [...line.matchAll(/'([^']*)'/g)].map(m => m[1]);
          for (const v of vals) {
            if (v.match(/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/)) { ip = v; break; }
          }
        }

        // Fallback: düz email/IP
        if (!email) {
          const m = line.match(/([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/);
          if (m) email = m[1];
        }
        if (!ip) {
          const m = line.match(/(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})/);
          if (m) ip = m[1];
        }
      }

      matches.push({
        email_masked: maskEmail(email),
        ip_masked: maskIp(ip),
        connections_apps,
        username,
        discriminator,
        avatar_hash,
        bio,
        premium,
        verified
      });

      if (matches.length >= maxHits) break;
    }
    rl.close();
    rs.close();
  } catch (err) {
    console.error(`[Hata] ${sqlPath}:`, err.message);
  }

  console.log(`[Tarama] Bitti: ${path.basename(sqlPath)} - ${matches.length} sonuç`);
  return matches;
}

// Platform URL oluşturucu (top-level)
function getConnectionUrl(app, id, name) {
  const appLower = (app || '').toLowerCase();
  if (appLower.includes('spotify')) return `https://open.spotify.com/user/${id || name}`;
  if (appLower.includes('github')) return `https://github.com/${name || id}`;
  if (appLower.includes('twitter') || appLower.includes('x')) return `https://twitter.com/${name || id}`;
  if (appLower.includes('instagram')) return `https://instagram.com/${name || id}`;
  if (appLower.includes('reddit')) return `https://reddit.com/user/${name || id}`;
  if (appLower.includes('steam')) return `https://steamcommunity.com/profiles/${id}`;
  if (appLower.includes('twitch')) return `https://twitch.tv/${name || id}`;
  if (appLower.includes('youtube')) return `https://youtube.com/channel/${id}`;
  if (appLower.includes('paypal')) return null;
  if (appLower.includes('ebay')) return `https://ebay.com/usr/${name || id}`;
  if (appLower.includes('facebook')) return `https://facebook.com/${id || name}`;
  if (appLower.includes('tiktok')) return `https://tiktok.com/@${name || id}`;
  if (appLower.includes('discord')) return null;
  if (appLower.includes('battle.net') || appLower.includes('battlenet')) return null;
  if (appLower.includes('epic')) return null;
  if (appLower.includes('riot')) return null;
  if (appLower.includes('crunchyroll')) return null;
  return null;
}

const app = express();
app.disable('x-powered-by');

// Security headers
app.use(helmet({
  contentSecurityPolicy: false, // Allow inline scripts for the UI
  crossOriginEmbedderPolicy: false
}));

// CORS
app.use(cors({
  origin: true,
  credentials: true,
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

// Rate limiting
const apiLimiter = rateLimit({
  windowMs: 60 * 1000, // 1 dakika
  max: 60,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'too_many_requests', message: 'Çok fazla istek. Lütfen bekleyin.' }
});
app.use('/api/', apiLimiter);

app.use(express.json({ limit: '1mb' }));
app.use(session({
  secret: 'zagros-session-secret-v2',
  resave: false,
  saveUninitialized: true,
  cookie: {
    httpOnly: true,
    secure: false, // Local development için false
    sameSite: 'lax',
    maxAge: 24 * 60 * 60 * 1000 // 24 saat
  }
}));

// 🕵️ ZİYARETÇİ LOGGING - Discord Webhook
const DISCORD_WEBHOOK_URL = process.env.DISCORD_WEBHOOK_URL || 'https://discord.com/api/webhooks/1496280136901722222/TGXA8J1SmCeDge4FNYoiP_pj1nCn4yK-FNp9dAP1MWP96EWPusk1JD0zXi-9BSjUZPyB';

async function logVisitor(req, action = 'visit') {
  try {
    const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'Unknown';
    const userAgent = req.headers['user-agent'] || 'Unknown';
    const browser = parseBrowser(userAgent);
    const timestamp = new Date().toLocaleString('tr-TR', { timeZone: 'Europe/Istanbul' });
    
    // IP'den konum bilgisi al
    let locationInfo = {};
    try {
      const geo = geoip.lookup(ip.split(',')[0].trim());
      if (geo) {
        locationInfo = {
          country: geo.country,
          city: geo.city,
          region: geo.region,
          isp: geo.isp,
          ll: geo.ll
        };
      }
    } catch (e) { /* ignore */ }
    
    const locationStr = locationInfo.city && locationInfo.country 
      ? `${locationInfo.city}, ${locationInfo.country}`
      : 'Bilinmiyor';
    
    const embed = {
      title: action === 'login' ? '🔐 Giriş Yapıldı' : '👁️ Site Ziyareti',
      color: action === 'login' ? 0x00FF00 : 0x5865F2,
      timestamp: new Date().toISOString(),
      fields: [
        {
          name: '🌐 IP Adresi',
          value: `\`\`\`${ip}\`\`\``,
          inline: true
        },
        {
          name: '📍 Konum',
          value: locationStr,
          inline: true
        },
        {
          name: '🌎 Ülke/Bölge',
          value: `${locationInfo.country || '?'}/${locationInfo.region || '?'}/${locationInfo.city || '?'}`
        },
        {
          name: '💻 Tarayıcı',
          value: browser,
          inline: true
        },
        {
          name: '📱 User-Agent',
          value: userAgent.length > 100 ? userAgent.substring(0, 100) + '...' : userAgent
        },
        {
          name: '🔗 Endpoint',
          value: req.path,
          inline: true
        }
      ],
      footer: {
        text: `Zagros OSINT - ${timestamp}`
      }
    };
    
    // Discord webhook'a gönder
    await axios.post(DISCORD_WEBHOOK_URL, {
      embeds: [embed]
    }, { timeout: 5000 });
    
    console.log(`[Visitor Log] ${action}: ${ip} - ${locationStr}`);
  } catch (err) {
    console.log('[Visitor Log Error]', err.message);
  }
}

function parseBrowser(userAgent) {
  if (!userAgent) return 'Unknown';
  if (userAgent.includes('Chrome')) return 'Chrome';
  if (userAgent.includes('Firefox')) return 'Firefox';
  if (userAgent.includes('Safari') && !userAgent.includes('Chrome')) return 'Safari';
  if (userAgent.includes('Edge')) return 'Edge';
  if (userAgent.includes('Opera')) return 'Opera';
  if (userAgent.includes('Discord')) return 'Discord Bot';
  return 'Other';
}

// Tüm API isteklerini logla (rate limit ile)
const loggedIps = new Set();
setInterval(() => loggedIps.clear(), 60000); // Her dakika reset

app.use((req, res, next) => {
  // Sadece API endpoint'lerini ve ana sayfayı logla
  const shouldLog = req.path === '/' || req.path.startsWith('/api/');
  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  
  if (shouldLog && !loggedIps.has(ip)) {
    loggedIps.add(ip);
    logVisitor(req, 'visit');
  }
  next();
});

app.post('/api/login', async (req, res) => {
  const password = String(req.body?.password ?? '');
  
  // Şifre ile giriş
  if (password === SITE_PASSWORD) {
    req.session.authed = true;
    req.session.discord_id = null;
    // Giriş başarılı - logla
    logVisitor(req, 'login');
    return res.json({ ok: true, method: 'password' });
  }
  
  return res.status(401).json({ ok: false, error: 'invalid_credentials' });
});

app.post('/api/logout', (req, res) => {
  req.session.destroy(() => res.json({ ok: true }));
});

app.get('/api/health', (req, res) => {
  // Public endpoint - sadece sunucu durumunu döndür
  return res.json({ 
    ok: true, 
    authed: req.session?.authed || false,
    timestamp: Date.now()
  });
});

app.use('/api', (req, res, next) => {
  if (req.session?.authed) return next();
  return res.status(401).json({ error: 'unauthorized' });
});

// 📧 EMAIL OSINT ENDPOINT - IntelX tarzı breach ve reputation raporu
app.get('/api/email-osint', async (req, res) => {
  const email = req.query.email;

  if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    return res.status(400).json({ error: 'invalid_email', message: 'Geçerli bir email adresi girin' });
  }

  try {
    console.log(`[EmailOSINT] Araştırma başlatıldı: ${email}`);
    const results = await performEmailOSINT(email);
    console.log(`[EmailOSINT] Tamamlandı: ${email} - ${results.breaches?.length || 0} breach, risk: ${results.summary.risk_level}`);
    return res.json(results);
  } catch (error) {
    console.error('[EmailOSINT] Hata:', error.message);
    return res.status(500).json({ error: 'osint_failed', message: 'Email araştırması başarısız oldu' });
  }
});

app.post('/api/guilds-enrich', async (req, res) => {
  if (Date.now() < findCordRateLimitedUntil) {
    return res.json({
      count: 0,
      guilds: [],
      rate_limited: true,
      retry_after_ms: Math.max(0, findCordRateLimitedUntil - Date.now())
    });
  }

  const ids = Array.isArray(req.body?.ids) ? req.body.ids : [];
  const guildItems = Array.isArray(req.body?.guilds) ? req.body.guilds : [];

  const requested = [];
  for (const it of guildItems) {
    const id = String(it?.id ?? '').trim();
    if (!/^\d{10,30}$/.test(id)) continue;
    const samples = Array.isArray(it?.sample_member_ids) ? it.sample_member_ids : (Array.isArray(it?.samples) ? it.samples : []);
    // Artık 12 sample member alıyoruz (daha fazla şans için)
    requested.push({ id, sample_member_ids: samples.map(x => String(x ?? '').trim()).filter(x => /^\d{10,30}$/.test(x) && !x.startsWith('7656119')).slice(0, 12) });
  }
  for (const idRaw of ids) {
    const id = String(idRaw ?? '').trim();
    if (!/^\d{10,30}$/.test(id)) continue;
    requested.push({ id, sample_member_ids: [] });
  }

  // dedupe
  const byId = new Map();
  for (const r of requested) {
    const prev = byId.get(r.id);
    if (!prev) byId.set(r.id, r);
    else {
      const mergedSamples = [...new Set([...(prev.sample_member_ids || []), ...(r.sample_member_ids || [])])].slice(0, 12);
      prev.sample_member_ids = mergedSamples;
    }
  }
  const unique = [...byId.values()].slice(0, 50);
  if (unique.length === 0) return res.json({ count: 0, guilds: [] });

  const concurrency = 2; // Rate limit'den kaçınmak için düşürüldü
  const enriched = [];
  let idx = 0;

  async function worker() {
    while (idx < unique.length) {
      const item = unique[idx++];
      const id = item.id;
      try {
        let name = null;
        let icon = null;
        let banner = null;

        // NOT: FindCord /api/user/{id} sadece USER ID'ler için çalışır
        // Guild ID'yi doğrudan sorgulamıyoruz, sadece sample member IDs üzerinden buluyoruz
        
        // Sample member IDs üzerinden guild'i bul
        if (item.sample_member_ids && item.sample_member_ids.length > 0) {
          for (const mid of item.sample_member_ids) {
            // Rate limit kontrolü
            if (Date.now() < findCordRateLimitedUntil) {
              console.log(`[Guilds Enrich] Rate limited, stopping`);
              break;
            }
            
            const mfc = await getFindCordData(mid);
            if (!mfc) continue;
            const mGuilds = mfc.Guilds || mfc.guilds;
            if (!Array.isArray(mGuilds)) continue;
            
            // Bu kullanıcının katıldığı guild'lerde ara
            const mg = mGuilds.find(g => 
              String(g.GuildId ?? g.guild_id ?? g.id ?? '') === String(id)
            );
            
            if (mg) {
              name = mg.GuildName || mg.guild_name || mg.name || name;
              icon = mg.GuildIcon || mg.guild_icon || mg.icon || mg.Icon || icon;
              banner = mg.GuildBanner || mg.guild_banner || mg.banner || mg.Banner || banner;
              console.log(`[Guilds Enrich] Found ${id} via member ${mid}: ${name}`);
              break;
            }
            
            // Rate limit'den kaçınmak için bekleme
            await new Promise(r => setTimeout(r, 100));
          }
        }

        const out = { id, name, icon, banner };
        if (icon) {
          if (icon.startsWith('http')) out.icon_url = icon;
          else out.icon_url = `https://cdn.discordapp.com/icons/${id}/${icon}.png?size=128`;
        }
        if (banner) {
          if (banner.startsWith('http')) out.banner_url = banner;
          else out.banner_url = `https://cdn.discordapp.com/banners/${id}/${banner}.png?size=512`;
        }
        if (out.name || out.icon_url || out.banner_url) enriched.push(out);
      } catch {
        // ignore individual failures
      }
    }
  }

  const workers = Array.from({ length: Math.min(concurrency, unique.length) }, () => worker());
  await Promise.all(workers);

  return res.json({ count: enriched.length, guilds: enriched, rate_limited: Date.now() < findCordRateLimitedUntil });
});

app.get('/api/search', async (req, res) => {
  const discordId = String(req.query?.discord_id ?? '').trim();
  if (!discordId || !/\d{5,30}$/.test(discordId)) {
    return res.status(400).json({ error: 'invalid_discord_id' });
  }

  const [txtMatches, ...sqlMatchLists] = await Promise.all([
    searchTxtByDiscordId(discordId),
    ...SQL_PATHS.map((p) => scanSqlFileForDiscordId(p, discordId)),
    getFindCordData(discordId)
  ]);
  const findCordData = sqlMatchLists.pop(); // Son eleman FindCord sonucu

  const allRaw = [...txtMatches, ...sqlMatchLists.flat()];

  // Tüm sonuçları birleştir — en zengin veriyi tek kartta topla
  const merged = {
    discord_id: discordId,
    username: null,
    discriminator: null,
    email: null,
    ip: null,
    ip_location: null,
    registration_ip: null,
    last_ip: null,
    avatar_hash: null,
    bio: null,
    premium: null,
    verified: null,
    subscription_type: null,
    is_active: null,
    created_at: null,
    last_login: null,
    connections_apps: [],
    sources: []
  };

  for (const item of allRaw) {
    if (item.username && item.username !== 'Bilinmeyen Kullanıcı') merged.username = item.username;
    if (item.discriminator) merged.discriminator = item.discriminator;
    if (item.email_masked) merged.email = item.email_masked;
    if (item.ip_masked && !merged.ip) merged.ip = item.ip_masked;
    if (item.registration_ip_masked && !merged.registration_ip) merged.registration_ip = item.registration_ip_masked;
    if (item.last_ip_masked && !merged.last_ip) merged.last_ip = item.last_ip_masked;
    if (item.avatar_hash && item.avatar_hash !== 'N/A') merged.avatar_hash = item.avatar_hash;
    if (item.bio && item.bio !== 'null') merged.bio = item.bio;
    if (item.premium !== null && merged.premium === null) merged.premium = item.premium;
    if (item.verified !== null && merged.verified === null) merged.verified = item.verified;
    if (item.subscription_type) merged.subscription_type = item.subscription_type;
    if (item.is_active !== null && merged.is_active === null) merged.is_active = item.is_active;
    if (item.created_at) merged.created_at = item.created_at;
    if (item.last_login) merged.last_login = item.last_login;
    // Bağlantıları birleştir (tekrarsız, app adına göre)
    if (Array.isArray(item.connections_apps)) {
      for (const c of item.connections_apps) {
        const key = typeof c === 'object' ? c.app : String(c);
        if (!merged.connections_apps.some(x => (typeof x === 'object' ? x.app : String(x)) === key)) {
          merged.connections_apps.push(c);
        }
      }
    }
  }

  // IP konum
  const ipForGeo = merged.ip || merged.last_ip || merged.registration_ip;
  if (ipForGeo) merged.ip_location = getIpLocation(ipForGeo);

  // Kaynak her zaman Zagros
  merged.sources = ['Zagros'];

  // Potansiyel arkadaşları bul (aynı IP veya guild'den)
  const potentialFriends = [];
  if (merged.ip || merged.findcord_servers?.length > 0) {
    const friendCandidates = new Map();
    
    // Aynı IP'den kayıtları bul
    if (merged.ip) {
      for (const sqlPath of SQL_PATHS) {
        try {
          if (!fs.existsSync(sqlPath)) continue;
          const content = await fs.promises.readFile(sqlPath, 'utf8');
          const lines = content.split('\n');
          
          for (const line of lines) {
            if (line.includes(merged.ip) && !line.includes(discordId)) {
              // Discord ID ve email çıkar
              const idMatch = line.match(/'(\d{10,20})'/);
              const emailMatch = line.match(/'([^']+@[^']+)'/);
              
              if (idMatch && idMatch[1] !== discordId) {
                const friendId = idMatch[1];
                const friendEmail = emailMatch ? emailMatch[1] : null;
                
                if (!friendCandidates.has(friendId)) {
                  friendCandidates.set(friendId, {
                    discord_id: friendId,
                    email: friendEmail,
                    relation: 'same_ip',
                    common_ip: merged.ip,
                    confidence: 'high'
                  });
                }
              }
            }
          }
        } catch { /* ignore */ }
      }
    }
    
    // FindCord guild'lerinden üyeleri kontrol et - AGRESİF ARAMA
    if (merged.findcord_servers?.length > 0) {
      console.log(`[Potansiyel Arkadaş] ${merged.findcord_servers.length} FindCord sunucusu bulundu`);
      
      for (const guild of merged.findcord_servers) {
        console.log(`[Potansiyel Arkadaş] Sunucu aranıyor: ${guild.name} (ID: ${guild.id})`);
        
        for (const sqlPath of SQL_PATHS) {
          if (!fs.existsSync(sqlPath)) continue;
          
          try {
            // Satır satır oku (performans için)
            const rs = fs.createReadStream(sqlPath, { encoding: 'utf8' });
            const rl = readline.createInterface({ input: rs, crlfDelay: Infinity });
            let lineCount = 0;
            
            for await (const line of rl) {
              lineCount++;
              // Sunucu ID'si veya ismi geçen satırları bul
              if (line.includes(guild.id) || (guild.name && line.toLowerCase().includes(guild.name.toLowerCase()))) {
                // Discord ID pattern'ini ara
                const idMatches = line.match(/'(\d{17,20})'/g);
                if (idMatches) {
                  for (const idMatch of idMatches) {
                    const cleanId = idMatch.replace(/'/g, '');
                    if (cleanId !== discordId && !friendCandidates.has(cleanId) && /\d{17,20}/.test(cleanId)) {
                      console.log(`[Potansiyel Arkadaş] Bulundu: ${cleanId} (${guild.name})`);
                      friendCandidates.set(cleanId, {
                        discord_id: cleanId,
                        relation: 'same_guild',
                        guild_name: guild.name,
                        guild_id: guild.id,
                        confidence: 'medium'
                      });
                    }
                  }
                }
              }
              if (lineCount % 50000 === 0) {
                console.log(`[Potansiyel Arkadaş] ${path.basename(sqlPath)}: ${lineCount} satır tarandı`);
              }
            }
            rl.close();
          } catch (err) {
            console.error(`[Hata] ${sqlPath}:`, err.message);
          }
        }
      }
      console.log(`[Potansiyel Arkadaş] Toplam bulunan: ${friendCandidates.size}`);
    }
    
    // Sonuçları diziye çevir (max 10)
    potentialFriends.push(...Array.from(friendCandidates.values()).slice(0, 10));
  }
  
  // Potansiyel arkadaşların detaylı bilgilerini çek
  for (const friend of potentialFriends) {
    try {
      // TXT dosyasından ara
      if (fs.existsSync(TXT_PATH)) {
        const content = await fs.promises.readFile(TXT_PATH, 'utf8');
        const obj = safeJsonParse(content);
        if (Array.isArray(obj?.users)) {
          for (const u of obj.users) {
            if (String(u?.discord_id ?? '') === friend.discord_id) {
              friend.username = u.username || friend.username;
              friend.email = u.email || friend.email;
              friend.found_in = 'TXT';
              break;
            }
          }
        }
      }
      
      // SQL dosyalarından ara
      if (!friend.email) {
        for (const sqlPath of SQL_PATHS) {
          if (!fs.existsSync(sqlPath)) continue;
          try {
            const content = await fs.promises.readFile(sqlPath, 'utf8');
            const lines = content.split('\n');
            for (const line of lines) {
              if (line.includes(friend.discord_id)) {
                const emailMatch = line.match(/'([^']+@[^']+)'/);
                const usernameMatch = line.match(/'([^']{2,30})'[^']*?,[^']*?'/);
                if (emailMatch) friend.email = emailMatch[1];
                if (usernameMatch && !friend.username) friend.username = usernameMatch[1];
                if (!friend.found_in) friend.found_in = path.basename(sqlPath);
                break;
              }
            }
          } catch { /* ignore */ }
          if (friend.email) break;
        }
      }
    } catch { /* ignore */ }
  }
  
  merged.potential_friends = potentialFriends;

  // FindCord API verisini ekle
  if (findCordData) {
    merged.findcord = findCordData;
    const ui = findCordData.UserInfo || findCordData.userInfo || {};

    // FindCord UserInfo ile boş alanları doldur
    if (!merged.username && ui.UserName) merged.username = ui.UserName;
    if (!merged.discriminator && ui.LegacyUserName) {
      const discMatch = ui.LegacyUserName.match(/#(\d+)$/);
      if (discMatch) merged.discriminator = discMatch[1];
    }
    if (ui.UserGlobalName) merged.findcord_global_name = ui.UserGlobalName;
    if (ui.UserdisplayAvatar) merged.findcord_avatar_url = ui.UserdisplayAvatar;
    if (!merged.bio && ui.UserBio) merged.bio = ui.UserBio;
    if (ui.UserBanner) merged.findcord_banner_url = ui.UserBanner;
    if (ui.UserPronouns) merged.findcord_pronouns = ui.UserPronouns;
    if (ui.UserCreated) merged.findcord_created = ui.UserCreated;
    if (ui.Presence) merged.findcord_presence = ui.Presence;
    // Avatar hash'i URL'den çıkar
    if (!merged.avatar_hash && ui.UserdisplayAvatar) {
      const avMatch = ui.UserdisplayAvatar.match(/\/avatars\/\d+\/(a?_\w+)\./);
      if (avMatch) merged.avatar_hash = avMatch[1];
    }
    // Badges
    if (Array.isArray(ui.UserBadge) && ui.UserBadge.length > 0) {
      merged.findcord_badges = ui.UserBadge.map(b => ({
        id: b.id,
        description: b.description,
        icon: b.icon ? `https://cdn.discordapp.com/badge-icons/${b.icon}.png` : null
      }));
    }
    // Guilds
    if (Array.isArray(findCordData.Guilds) && findCordData.Guilds.length > 0) {
      merged.findcord_servers = findCordData.Guilds.map(g => ({
        id: g.GuildId,
        name: g.GuildName,
        icon: g.GuildIcon || null,
        banner: g.GuildBanner || null,
        display_name: g.displayName || null,
        booster: g.Booster || false,
        join_time: g.JoinTime || null,
        roles: Array.isArray(g.Roles) ? g.Roles.map(r => ({
          id: r.id,
          name: r.name,
          color: r.color || null,
          icon: r.icon || null
        })) : [],
        stats: g.UserStats || null
      }));
    }
    // Ek bilgiler
    if (findCordData.TopName) merged.findcord_top_name = findCordData.TopName;
    if (findCordData.TopAge) merged.findcord_top_age = findCordData.TopAge;
    if (findCordData.TopSex) merged.findcord_top_sex = findCordData.TopSex;
    if (Array.isArray(findCordData.displayNames)) merged.findcord_display_names = findCordData.displayNames;
    if (Array.isArray(findCordData.Punishments) && findCordData.Punishments.length > 0) {
      merged.findcord_punishments = findCordData.Punishments;
    }
  }

  const results = {
    discord_id: discordId,
    result: allRaw.length > 0 || findCordData ? merged : null
  };

  return res.json(results);
});

// Email veya IP ile arama
async function searchTxtByField(field, value) {
  if (!fs.existsSync(TXT_PATH)) return [];
  const content = await fs.promises.readFile(TXT_PATH, 'utf8');
  const obj = safeJsonParse(content);
  const users = Array.isArray(obj?.users) ? obj.users : [];
  const val = String(value).toLowerCase();
  return users.filter(u => {
    const v = String(u?.[field] ?? '').toLowerCase();
    return v === val || v.includes(val);
  }).map(u => ({
    source: 'Zagros',
    discord_id: String(u.discord_id ?? ''),
    username: u.username ?? null,
    discriminator: u.discriminator ?? null,
    email_masked: maskEmail(u.email ?? null),
    registration_ip_masked: maskIp(u.registration_ip ?? null),
    last_ip_masked: maskIp(u.last_ip ?? null),
    created_at: u.created_at ?? null,
    last_login: u.last_login ?? null,
    subscription_type: u.subscription_type ?? null,
    is_active: u.is_active ?? null
  }));
}

async function scanSqlFileForField(sqlPath, field, value, maxHits = 30) {
  if (!fs.existsSync(sqlPath)) return [];
  const matches = [];
  try {
    const rs = fs.createReadStream(sqlPath, { encoding: 'utf8' });
    const rl = readline.createInterface({ input: rs, crlfDelay: Infinity });
    const needle = String(value);

    for await (const line of rl) {
      if (!line.includes(needle)) continue;

      let email = null, ip = null, username = null, discriminator = null;
      let discord_id = null, connections_apps = [], avatar_hash = null;

      // Discord ID çıkar (tuple veya JSON)
      const idTuple = line.match(/\(\s*(\d{10,20})\s*,/);
      if (idTuple) discord_id = idTuple[1];
      const idJson = line.match(/"discord_id"\s*:\s*"(\d{10,20})"/);
      if (!discord_id && idJson) discord_id = idJson[1];
      const idSearched = line.match(/"searched_discord_id"\s*:\s*"(\d{10,20})"/);
      if (!discord_id && idSearched) discord_id = idSearched[1];

      const rawEmail = extractField(line, 'email');
      if (rawEmail) email = decodeBase64Maybe(rawEmail);
      const rawIp = extractField(line, 'ip');
      if (rawIp && !rawIp.match(/^[a-f0-9]{32}$/)) ip = rawIp;
      const rawUser = extractField(line, 'username');
      if (rawUser && rawUser !== 'N/A' && rawUser !== 'N\\/A') username = rawUser;
      const rawDisc = extractField(line, 'discriminator');
      if (rawDisc && rawDisc !== 'N/A' && rawDisc !== 'N\\/A') discriminator = rawDisc;
      const rawAvatar = extractField(line, 'avatar_hash');
      if (rawAvatar && rawAvatar !== 'N/A' && rawAvatar !== 'N\\/A') avatar_hash = rawAvatar;

      connections_apps = extractConnectionsFromLine(line);

      // Tuple format: (id, 'base64email', ...)
      if (!email && line.match(/\(\s*\d+\s*,\s*'/)) {
        const tupleMatch = line.match(/\(\s*\d+\s*,\s*'([^']+)'/);
        if (tupleMatch) email = decodeBase64Maybe(tupleMatch[1]);
      }

      // Fallback
      if (!email) {
        const m = line.match(/([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/);
        if (m) email = m[1];
      }
      if (!ip) {
        const m = line.match(/(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})/);
        if (m) ip = m[1];
      }

      matches.push({
        discord_id, email_masked: maskEmail(email), ip_masked: maskIp(ip),
        connections_apps, username, discriminator, avatar_hash
      });
      if (matches.length >= maxHits) break;
    }
    rl.close(); rs.close();
  } catch (err) {
    console.error(`[Hata] ${sqlPath}:`, err.message);
  }
  return matches;
}

app.get('/api/search-email', async (req, res) => {
  const email = String(req.query?.email ?? '').trim().toLowerCase();
  if (!email || email.length < 3) return res.status(400).json({ error: 'invalid_email' });

  const breaches = [];

  // === TXT dosyası: dcıdsorgudata ===
  if (fs.existsSync(TXT_PATH)) {
    try {
      const content = await fs.promises.readFile(TXT_PATH, 'utf8');
      const obj = safeJsonParse(content);
      const users = Array.isArray(obj?.users) ? obj.users : [];
      const val = email.toLowerCase();
      for (const u of users) {
        if (String(u?.email ?? '').toLowerCase().includes(val)) {
          const ip = u.registration_ip || u.last_ip || null;
          breaches.push({
            source: 'Zagros',
            site: 'Zagros',
            username: u.username || null,
            discord_id: String(u.discord_id ?? ''),
            email: u.email || null,
            ip: ip,
            ip_location: ip ? getIpLocation(ip) : null,
            registration_ip: u.registration_ip || null,
            last_ip: u.last_ip || null,
            subscription_type: u.subscription_type || null,
            is_active: u.is_active ?? null,
            created_at: u.created_at || null,
            last_login: u.last_login || null,
            connections_apps: [],
            avatar_hash: null,
            bio: null
          });
        }
      }
    } catch { /* ignore */ }
  }

  // === SQL dosyaları: her biri ayrı breach ===
  // OSINT tarzı: kaynak isimleri gizli, hepsi Zagros olarak gösterilir
  const sourceNames = {
    'discord data.sql': { source: 'Zagros', site: 'Zagros' },
    'idsorgu(1).sql': { source: 'Zagros', site: 'Zagros' },
    '840k.sql': { source: 'Zagros', site: 'Zagros' }
  };

  // Email'in base64 hallerini de dene
  const b64Full = Buffer.from(email, 'utf8').toString('base64');
  const needles = [email, b64Full];

  for (const sqlPath of SQL_PATHS) {
    if (!fs.existsSync(sqlPath)) continue;
    const baseName = path.basename(sqlPath);
    const sourceInfo = sourceNames[baseName] || { source: 'Zagros', site: 'Zagros' };

    try {
      const rs = fs.createReadStream(sqlPath, { encoding: 'utf8' });
      const rl = readline.createInterface({ input: rs, crlfDelay: Infinity });

      for await (const line of rl) {
        // Satır email veya base64'email içermeli
        let matched = false;
        for (const n of needles) { if (line.includes(n)) { matched = true; break; } }
        if (!matched) continue;

        // scanSqlFileForDiscordId ile aynı çıkarma mantığını kullan
        let foundEmail = null, username = null, discord_id = null;
        let ip = null, avatar_hash = null, bio = null;
        let discriminator = null, premium = null, verified = null;
        let connections_apps = [], created_at = null, last_login = null;
        let subscription_type = null, is_active = null;
        let registration_ip = null, last_ip = null;
        let isUsersTable = false;

        // === FORMAT 1: users tablosu INSERT ===
        if (line.includes('INSERT INTO') && (line.includes('`users`') || line.includes('users'))) {
          isUsersTable = true;
          const vals = [...line.matchAll(/'([^']*)'/g)].map(m => m[1]);
          if (vals.length >= 6) {
            username = vals[2] || null;
            discriminator = vals[3] || null;
            foundEmail = vals[4] || null;
            avatar_hash = vals[5] || null;
            for (let vi = vals.length - 1; vi >= 0; vi--) {
              if (vals[vi].match(/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/)) {
                if (!registration_ip) registration_ip = vals[vi];
                else if (!last_ip) last_ip = vals[vi];
              }
            }
            ip = last_ip || registration_ip;
          }
        }

        // === FORMAT 1b: discord_ids tablosu tuple ===
        if (!isUsersTable && line.match(/\(\s*\d{10,20}\s*,/)) {
          const tupleMatch = line.match(/\(\s*(\d{10,20})\s*,/);
          if (tupleMatch) discord_id = tupleMatch[1];
          const tupleVals = [...line.matchAll(/'([^']*)'/g)].map(m => m[1]);
          if (tupleVals.length >= 1) foundEmail = decodeBase64Maybe(tupleVals[0]);
          if (tupleVals.length >= 5) {
            const candidate = tupleVals[4];
            if (candidate && (candidate.match(/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/) || candidate.includes(':'))) {
              ip = candidate;
            }
          }
          // Diğer IP'leri de ara
          if (!ip) {
            for (const v of tupleVals) {
              if (v.match(/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/)) { ip = v; break; }
              if (v.match(/^[0-9a-f]{1,4}(:[0-9a-f]{1,4}){2,7}$/i)) { ip = v; break; }
            }
          }
        }

        // === FORMAT 2: query_logs JSON (response_data) ===
        // Hem "response_data" key'li hem de tuple içinde JSON'ı yakala
        {
          let jsonStr = null;
          // response_data key ile
          if (line.includes('"response_data"') || line.includes("'response_data'")) {
            const jsonPatterns = [
              /'(\{[^']*\})'/,
              /"response_data"\s*:\s*"(\{.*?\})"\s*[,}]/
            ];
            for (const pat of jsonPatterns) {
              const m = line.match(pat);
              if (m) { jsonStr = m[1]; break; }
            }
          }
          // Tuple içinde JSON: (id, user_id, 'discord_id', 'ip', 'ua', '{...}', ...)
          if (!jsonStr && line.match(/\(\s*\d+\s*,\s*\d+\s*,\s*'\d+/)) {
            const tupleIdMatch = line.match(/\(\s*\d+\s*,\s*\d+\s*,\s*'(\d{10,20})'/);
            if (tupleIdMatch) discord_id = tupleIdMatch[1];
            // JSON string'i bul: tek tırnak içinde { ile başlayan
            const jsonInTuple = line.match(/'(\{"[^']*\})'/);
            if (jsonInTuple) jsonStr = jsonInTuple[1];
          }

          if (jsonStr) {
            const rawJson = jsonStr.replace(/\\"/g, '"').replace(/\\\\/g, '\\');
            const parsed = safeJsonParse(rawJson);
            if (parsed) {
              const d = parsed.data || parsed;
              if (d.email) foundEmail = decodeBase64Maybe(d.email);
              if (d.username && d.username !== 'N/A' && d.username !== 'N\\/A') username = d.username;
              if (d.discord_id) discord_id = d.discord_id;
              if (d.discriminator && d.discriminator !== '0' && d.discriminator !== 'N/A') discriminator = d.discriminator;
              if (d.avatar_hash && d.avatar_hash !== 'N/A' && d.avatar_hash !== 'N\\/A') avatar_hash = d.avatar_hash;
              if (d.bio && d.bio !== 'null' && d.bio !== 'N/A') bio = d.bio;
              if (d.premium !== undefined && d.premium !== null) premium = String(d.premium);
              if (d.verified !== undefined && d.verified !== null) verified = String(d.verified);
              if (d.ip && !d.ip.match(/^[a-f0-9]{32}$/i)) ip = d.ip;
              if (d.registration_ip) registration_ip = d.registration_ip;
              if (d.last_ip) last_ip = d.last_ip;
              if (d.subscription_type) subscription_type = d.subscription_type;
              if (d.is_active !== undefined) is_active = d.is_active;
              if (d.created_at) created_at = d.created_at;
              if (d.last_login) last_login = d.last_login;
              if (d.connections && typeof d.connections === 'object' && !Array.isArray(d.connections)) {
                connections_apps = parseConnObj(d.connections);
              }
            }
          }
          // searched_discord_id
          const idSearched = line.match(/"searched_discord_id"\s*:\s*"(\d{10,20})"/);
          if (!discord_id && idSearched) discord_id = idSearched[1];
        }

        // === FORMAT 3: Basit tuple (users tablosu benzeri) ===
        if (!isUsersTable && !foundEmail && line.match(/\(\s*\d+\s*,/)) {
          const tupleVals = [...line.matchAll(/'([^']*)'/g)].map(m => m[1]);
          // Format: (id, 'discord_id', 'username', 'discriminator', 'email', 'avatar_hash', ...)
          if (tupleVals.length >= 5) {
            discord_id = tupleVals[1] || discord_id;
            username = tupleVals[2] || username;
            discriminator = tupleVals[3] || discriminator;
            foundEmail = tupleVals[4] || null;
            avatar_hash = tupleVals[5] || avatar_hash;
          }
        }

        // Email eşleşme kontrolü
        if (!foundEmail) continue;
        const emailLower = foundEmail.toLowerCase();
        if (!emailLower.includes(email) && email !== b64Full) continue;

        const finalIp = ip || last_ip || registration_ip;
        const hasData = username || discord_id || finalIp || avatar_hash || bio ||
                        (Array.isArray(connections_apps) && connections_apps.length > 0);
        if (!hasData) continue;

        breaches.push({
          source: sourceInfo.source,
          site: sourceInfo.site,
          username, discord_id, email: foundEmail,
          discriminator, avatar_hash, bio,
          premium, verified,
          ip: finalIp,
          ip_location: finalIp ? getIpLocation(finalIp) : null,
          registration_ip, last_ip,
          subscription_type, is_active, created_at, last_login,
          connections_apps
        });

        if (breaches.length > 50) break;
      }
      rl.close(); rs.close();
    } catch (err) {
      console.error(`[Hata] ${sqlPath}:`, err.message);
    }
  }

  // Dış OSINT kaynakları - paralel çalışsın
  const [githubResults, hibpBreaches, gravatarInfo, platformResults, emailrepInfo] = await Promise.all([
    searchGitHubByEmail(email),
    checkHaveIBeenPwned(email),
    getGravatarInfo(email),
    searchPlatformsByEmail(email),
    checkEmailrep(email)
  ]);

  // Discord kayıtlarını birleştir (aynı ID'li olanları tekilleştir)
  const seenDiscordIds = new Map();
  for (const b of breaches) {
    if (b.discord_id) {
      const existing = seenDiscordIds.get(b.discord_id);
      if (!existing) {
        seenDiscordIds.set(b.discord_id, { ...b, sources: [b.source || 'Zagros'] });
      } else {
        // En zengin veriyi tut, sources'u birleştir
        existing.sources.push(b.source || 'Zagros');
        if (!existing.username && b.username) existing.username = b.username;
        if (!existing.ip && b.ip) existing.ip = b.ip;
        if (!existing.connections_apps?.length && b.connections_apps?.length) {
          existing.connections_apps = b.connections_apps;
        }
      }
    }
  }
  
  // Site-kullanıcı listesi oluştur - birleştirilmiş Discord kayıtları
  const sites = [];
  for (const b of seenDiscordIds.values()) {
    sites.push({
      site: 'Discord',
      username: b.username,
      discord_id: b.discord_id,
      email: b.email,
      ip: b.ip,
      sources: [...new Set(b.sources)], // Tekrarları kaldır
      connections_apps: b.connections_apps || [],
      created_at: b.created_at
    });
    
    // Bağlantılı hesapları ekle
    if (Array.isArray(b.connections_apps)) {
      for (const c of b.connections_apps) {
        const app = typeof c === 'object' ? c.app : String(c);
        const connName = typeof c === 'object' ? c.name : '';
        const connId = typeof c === 'object' ? c.id : '';
        const connUsername = connName || connId || '-';
        sites.push({
          site: app.charAt(0).toUpperCase() + app.slice(1),
          username: connUsername,
          connection_id: connId,
          connection_name: connName,
          leak_type: 'connection',
          source_discord: b.discord_id,
          url: getConnectionUrl(app, connId, connName)
        });
      }
    }
  }
  // GitHub sonuçlarını ekle
  for (const g of githubResults) {
    sites.push({
      site: 'GitHub',
      username: g.username,
      name: g.name,
      url: g.url,
      avatar: g.avatar,
      bio: g.bio,
      location: g.location,
      company: g.company,
      blog: g.blog,
      public_repos: g.public_repos,
      followers: g.followers,
      following: g.following,
      created_at: g.created_at
    });
  }
  // HaveIBeenPwned breach'lerini ekle
  if (hibpBreaches && hibpBreaches.length > 0) {
    for (const breach of hibpBreaches) {
      sites.push({
        site: `Breach: ${breach.site}`,
        username: 'N/A',
        breach_date: breach.breach_date,
        description: breach.description,
        data_classes: breach.data_classes,
        is_sensitive: breach.is_sensitive,
        leak_type: 'breach'
      });
    }
  }
  // Gravatar sonuçlarını ekle
  if (gravatarInfo) {
    sites.push({
      site: 'Gravatar',
      username: gravatarInfo.username || 'N/A',
      name: gravatarInfo.name,
      avatar: gravatarInfo.avatar,
      profile_url: gravatarInfo.profile_url,
      urls: gravatarInfo.urls,
      accounts: gravatarInfo.accounts,
      leak_type: 'gravatar'
    });
  }
  
  // Platform sonuçlarını ekle (LinkedIn, Pinterest, TikTok, vb.)
  for (const p of platformResults) {
    sites.push({
      site: p.platform,
      username: p.username,
      url: p.url,
      note: p.note,
      confidence: p.confidence,
      leak_type: 'platform'
    });
  }

  // Email validasyon
  const validation = validateEmail(email);

  return res.json({
    query: email,
    type: 'email',
    validation,
    breaches_count: hibpBreaches?.length || 0,
    platforms_found: platformResults.length,
    emailrep: emailrepInfo,
    sites
  });
});

app.get('/api/search-ip', async (req, res) => {
  const ip = String(req.query?.ip ?? '').trim();
  if (!ip || ip.length < 5) return res.status(400).json({ error: 'invalid_ip' });

  const [txtMatches, ...sqlMatchLists] = await Promise.all([
    (async () => {
      if (!fs.existsSync(TXT_PATH)) return [];
      const content = await fs.promises.readFile(TXT_PATH, 'utf8');
      const obj = safeJsonParse(content);
      const users = Array.isArray(obj?.users) ? obj.users : [];
      return users.filter(u => u.registration_ip === ip || u.last_ip === ip).map(u => ({
        source: 'Zagros', discord_id: String(u.discord_id ?? ''), username: u.username,
        email_masked: maskEmail(u.email ?? null), registration_ip_masked: maskIp(u.registration_ip ?? null),
        last_ip_masked: maskIp(u.last_ip ?? null), created_at: u.created_at, subscription_type: u.subscription_type, is_active: u.is_active
      }));
    })(),
    ...SQL_PATHS.map(p => scanSqlFileForField(p, 'ip', ip))
  ]);
  const allRaw = [...txtMatches, ...sqlMatchLists.flat()];

  const seen = new Map();
  for (const item of allRaw) {
    const key = item.discord_id || item.email_masked;
    if (!key) continue;
    if (!seen.has(key)) seen.set(key, { discord_id: item.discord_id, username: item.username, discriminator: item.discriminator, email: item.email_masked, ip: item.ip_masked, ip_location: null, avatar_hash: item.avatar_hash, connections_apps: [], sources: ['Zagros'] });
    const m = seen.get(key);
    if (item.email_masked && !m.email) m.email = item.email_masked;
    if (item.avatar_hash && item.avatar_hash !== 'N/A') m.avatar_hash = item.avatar_hash;
    if (Array.isArray(item.connections_apps)) {
      for (const c of item.connections_apps) {
        const ck = typeof c === 'object' ? c.app : String(c);
        if (!m.connections_apps.some(x => (typeof x === 'object' ? x.app : String(x)) === ck)) m.connections_apps.push(c);
      }
    }
  }
  const results = [...seen.values()];
  for (const r of results) { if (r.ip) r.ip_location = getIpLocation(r.ip); }

  return res.json({ query: ip, type: 'ip', ip_location: getIpLocation(ip), count: results.length, results });
});

// Phone lookup endpoint
app.get('/api/lookup-phone', async (req, res) => {
  const phone = String(req.query?.phone ?? '').trim();
  if (!phone) return res.status(400).json({ error: 'invalid_phone' });
  
  const validation = validatePhone(phone);
  return res.json({
    query: phone,
    type: 'phone',
    validation
  });
});

// Domain lookup endpoint
app.get('/api/lookup-domain', async (req, res) => {
  const domain = String(req.query?.domain ?? '').trim();
  if (!domain || !domain.includes('.')) return res.status(400).json({ error: 'invalid_domain' });
  
  const info = await lookupDomain(domain);
  return res.json({
    query: domain,
    type: 'domain',
    info
  });
});

// Sunucu arama endpoint
app.get('/api/search-guild', async (req, res) => {
  const guildId = String(req.query?.guild_id ?? '').trim();
  if (!guildId || !/^\d{10,30}$/.test(guildId)) {
    return res.status(400).json({ error: 'invalid_guild_id' });
  }

  const guildInfo = {
    id: guildId,
    name: 'Bilinmeyen Sunucu',
    icon: null,
    banner: null,
    raw_findcord: false
  };
  
  console.log(`[Sunucu Sorgu] Başlıyor: ${guildId}`);

  // SQL dosyalarında bu sunucuya ait kayıtları ara
  const members = [];
  const seenIds = new Set();

  for (const sqlPath of SQL_PATHS) {
    if (!fs.existsSync(sqlPath)) continue;
    
    try {
      const rs = fs.createReadStream(sqlPath, { encoding: 'utf8' });
      const rl = readline.createInterface({ input: rs, crlfDelay: Infinity });
      
      for await (const line of rl) {
        // Bu dataset'te guild üyeliği genelde satır içinde string olarak tutuluyor: '[id,id,id]'
        if (!line.includes(guildId)) continue;

        // GuildId'nin geçtiği bracket array'i doğrula (false positive azalt)
        const bracketLists = [...line.matchAll(/\[(\d{10,30}(?:,\d{10,30})*)\]/g)].map(m => m[1]);
        const hasGuildInList = bracketLists.some(raw => raw.split(',').includes(guildId));
        if (!hasGuildInList) continue;

        const userIdMatch = line.match(/\(\s*(\d{17,20})\s*,/);
        const userId = userIdMatch?.[1];
        if (!userId || seenIds.has(userId)) continue;
        seenIds.add(userId);

        // SQL tuple içindeki tüm değerleri çıkar
        const quotedValues = [...line.matchAll(/'([^']*)'/g)].map(m => m[1]);
        const allValues = quotedValues;

        // Email bul - düz email veya base64
        let email = null;
        const emailMatches = line.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g);
        if (emailMatches?.length) {
          for (const em of emailMatches) {
            if (em.length > 5 && em.includes('.') && !em.includes('example.com')) { email = em; break; }
          }
        }
        if (!email) {
          const b64Candidate = quotedValues.find(v => v && v.length >= 8 && v.length <= 200 && /^[A-Za-z0-9+/=]+$/.test(v));
          if (b64Candidate) {
            const decoded = decodeBase64Maybe(b64Candidate);
            if (decoded && decoded.includes('@')) email = decoded;
          }
        }

        // IP bul
        let ip = null;
        const ipMatches = line.match(/(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})/g);
        if (ipMatches?.length) {
          for (const ipCandidate of ipMatches) {
            const parts = ipCandidate.split('.').map(Number);
            if (parts.every(p => p >= 0 && p <= 255)) { ip = ipCandidate; break; }
          }
        }

        // Username, avatar, global_name, connections ara
        let username = null;
        let avatar_hash = null;
        let global_name = null;
        let connections = [];
        let connection_types = [];
        let phone = null;

        // Username, global_name, avatar - Base64 decode dahil
        for (const val of quotedValues) {
          if (!val || val.length < 2) continue;

          // Base64 decode dene
          let decoded = val;
          const isBase64 = val.length >= 8 && /^[A-Za-z0-9+/=]+$/.test(val);
          if (isBase64) {
            const tryDecode = decodeBase64Maybe(val);
            if (tryDecode && tryDecode !== val) {
              decoded = tryDecode;
            }
          }

          // Username candidate (decode edilmiş değer)
          if (decoded.length > 2 && decoded.length < 50 && !decoded.includes('@') && !decoded.includes('.')) {
            if (!username && /^[a-zA-Z0-9_.]+$/.test(decoded)) {
              username = decoded;
              continue;
            }
            // Global name candidate (boşluk içerebilir)
            if (!global_name && decoded.includes(' ') && decoded.length > 3 && decoded.length < 50) {
              global_name = decoded;
              continue;
            }
            // Avatar hash: 32 karakter hex veya 'a_' ile başlayan
            if (!avatar_hash && (decoded.match(/^[a-f0-9]{32}$/) || decoded.match(/^a_[a-f0-9]{32}$/))) {
              avatar_hash = decoded;
              continue;
            }
            // Phone number
            if (!phone && decoded.match(/^\+?[0-9\s\-\(\)]{10,20}$/)) {
              phone = decoded;
            }
          }
        }

        // Connections çıkar - ["steam","twitch"] formatı
        const connMatch = line.match(/\[(?:"([^"]+)"|'([^']+)'|([^,\]]+))(?:,(?:"([^"]+)"|'([^']+)'|([^,\]]+)))*\]/g);
        if (connMatch) {
          for (const match of connMatch) {
            // Boş array değilse ve guild listesi değilse
            if (match.length > 2 && !match.includes(guildId)) {
              const connVals = [...match.matchAll(/"([^"]+)"|'([^']+)'/g)].map(m => m[1] || m[2]).filter(Boolean);
              if (connVals.length > 0 && connVals.length < 20) {
                // Bu bir connection listesi mi kontrol et
                const isConnection = connVals.some(v => ['steam', 'twitch', 'youtube', 'spotify', 'twitter', 'github', 'instagram', 'paypal'].includes(v.toLowerCase()));
                if (isConnection) {
                  connection_types = connVals.map(v => v.toLowerCase());
                  connections = connVals.map(v => ({ type: v.toLowerCase(), name: v }));
                }
              }
            }
          }
        }

        members.push({
          discord_id: userId,
          username,
          global_name,
          email,
          ip,
          phone,
          avatar_hash,
          connections,
          connection_types,
          source: path.basename(sqlPath)
        });
      }
      rl.close();
    } catch (err) {
      console.error(`[Hata] ${sqlPath}:`, err.message);
    }
  }

  // Bulunan ilk üye ile FindCord'dan sunucu bilgisi almaya çalış
  if (members.length > 0 && Date.now() >= findCordRateLimitedUntil) {
    try {
      const firstMember = members[0];
      console.log(`[Sunucu Sorgu] İlk üye ile FindCord sorgusu: ${firstMember.discord_id}`);
      const memberFindCord = await getFindCordData(firstMember.discord_id);
      
      if (memberFindCord && memberFindCord.Guilds) {
        const matchingGuild = memberFindCord.Guilds.find(g => 
          g.GuildId === guildId || g.id === guildId ||
          String(g.GuildId) === String(guildId) || String(g.id) === String(guildId)
        );
        if (matchingGuild) {
          guildInfo.name = matchingGuild.GuildName || matchingGuild.name || guildInfo.name;
          guildInfo.icon = matchingGuild.GuildIcon || matchingGuild.icon || guildInfo.icon;
          guildInfo.banner = matchingGuild.GuildBanner || matchingGuild.banner || guildInfo.banner;
          console.log(`[Sunucu Sorgu] Sunucu adı bulundu: ${guildInfo.name}`);
        }
      }
    } catch (err) {
      console.error(`[Sunucu Sorgu] FindCord hatası:`, err.message);
    }
  }
  
  // İlk 3 üye için FindCord'dan detaylı bilgi çek (rate limit için azaltıldı)
  const enrichCount = Math.min(3, members.length);
  console.log(`[Guild Search] Enriching ${enrichCount} members with FindCord data...`);
  for (let i = 0; i < enrichCount; i++) {
    // Rate limit kontrolü
    if (Date.now() < findCordRateLimitedUntil) {
      console.log(`[Guild Search] Rate limited, skipping enrichment`);
      break;
    }
    
    const member = members[i];
    if (!member.username || !member.avatar_hash) {
      try {
        const fcData = await getFindCordData(member.discord_id);
        if (fcData) {
          // FindCord'dan username al
          if (!member.username && (fcData.UserInfo?.username || fcData.username)) {
            member.username = fcData.UserInfo?.username || fcData.username;
            console.log(`[Guild Search] Found username for ${member.discord_id}: ${member.username}`);
          }
          // Global name
          if (!member.username && (fcData.UserInfo?.global_name || fcData.global_name)) {
            member.username = fcData.UserInfo?.global_name || fcData.global_name;
          }
          // Avatar
          if (!member.avatar_hash && (fcData.UserdisplayAvatar || fcData.avatar)) {
            member.avatar_hash = fcData.UserdisplayAvatar || fcData.avatar;
            console.log(`[Guild Search] Found avatar for ${member.discord_id}`);
          }
        }
        // Rate limit'den kaçınmak için kısa bekleme
        if (i < enrichCount - 1) {
          await new Promise(r => setTimeout(r, 200));
        }
      } catch (err) {
        console.log(`[Guild Search] FindCord error for ${member.discord_id}: ${err.message}`);
      }
    }
  }
  
  // Üye bilgilerini zenginleştir - avatar URL'leri oluştur
  for (const member of members) {
    if (member.avatar_hash) {
      const ext = member.avatar_hash.startsWith('a_') ? 'gif' : 'png';
      member.avatar_url = `https://cdn.discordapp.com/avatars/${member.discord_id}/${member.avatar_hash}.${ext}?size=128`;
    } else {
      // Varsayılan avatar
      member.avatar_url = `https://cdn.discordapp.com/embed/avatars/${parseInt(member.discord_id) % 5}.png`;
    }
  }

  // IP konum bilgisi ekle (ilk 20 üye için - rate limit koruması)
  const ipLocationCache = new Map();
  const membersWithLocation = [];
  
  for (let i = 0; i < Math.min(20, members.length); i++) {
    const member = members[i];
    if (member.ip && !ipLocationCache.has(member.ip)) {
      try {
        const loc = await getIpGeolocation(member.ip);
        if (loc) {
          ipLocationCache.set(member.ip, loc);
          member.ip_location = loc;
          console.log(`[IP-Location] ${member.ip} -> ${loc.city}, ${loc.district || 'N/A'} (${loc.lat}, ${loc.lon})`);
        }
      } catch (err) {
        console.log(`[IP-Location] Hata ${member.ip}:`, err.message);
      }
    } else if (member.ip && ipLocationCache.has(member.ip)) {
      member.ip_location = ipLocationCache.get(member.ip);
    }
    membersWithLocation.push(member);
  }

  // Cevap dön - TÜM üyeleri döndür (sadece konum olanları değil)
  return res.json({
    query: guildId,
    type: 'guild',
    guild: guildInfo,
    count: members.length,
    members: members, // Tüm üyeleri döndür
    has_locations: ipLocationCache.size > 0,
    location_count: ipLocationCache.size
  });
});

// Username OSINT endpoint
app.get('/api/lookup-username', async (req, res) => {
  const username = String(req.query?.username ?? '').trim();
  if (!username || username.length < 2) return res.status(400).json({ error: 'invalid_username' });
  
  const results = await searchUsername(username);
  return res.json({
    query: username,
    type: 'username',
    count: results.filter(r => r.available).length,
    platforms: results.length,
    results
  });
});

// Health check / Status endpoint (Real-time monitoring)
app.get('/api/status', async (req, res) => {
  const sources = [
    { name: 'GitHub', status: 'unknown', latency: null },
    { name: 'HaveIBeenPwned', status: 'unknown', latency: null },
    { name: 'Gravatar', status: 'unknown', latency: null },
    { name: 'Emailrep', status: 'unknown', latency: null },
    { name: 'FindCord', status: 'unknown', latency: null },
    { name: 'LocalDB', status: 'unknown', latency: null }
  ];
  
  // Her kaynağı test et
  for (const source of sources) {
    const start = Date.now();
    try {
      if (source.name === 'GitHub') {
        await axios.head('https://api.github.com', { timeout: 3000 });
        source.status = 'online';
      } else if (source.name === 'HaveIBeenPwned') {
        await axios.get('https://haveibeenpwned.com/api/v3/breach/test', { timeout: 3000, validateStatus: () => true });
        source.status = 'online';
      } else if (source.name === 'Gravatar') {
        await axios.head('https://gravatar.com', { timeout: 3000 });
        source.status = 'online';
      } else if (source.name === 'Emailrep') {
        await axios.head('https://emailrep.io', { timeout: 3000 });
        source.status = 'online';
      } else if (source.name === 'FindCord') {
        source.status = FINDCORD_API_KEY ? 'online' : 'offline';
      } else if (source.name === 'LocalDB') {
        const hasData = fs.existsSync(TXT_PATH) || SQL_PATHS.some(p => fs.existsSync(p));
        source.status = hasData ? 'online' : 'offline';
      }
      source.latency = Date.now() - start;
    } catch {
      source.status = 'offline';
      source.latency = null;
    }
  }
  
  const onlineCount = sources.filter(s => s.status === 'online').length;
  
  return res.json({
    timestamp: new Date().toISOString(),
    total: sources.length,
    online: onlineCount,
    offline: sources.length - onlineCount,
    health: onlineCount === sources.length ? '100%' : `${Math.round((onlineCount/sources.length)*100)}%`,
    sources
  });
});

// Tüm sunucuları listele - FINDCORD ENTegrasyonlu
app.post('/api/reload-sources', (req, res) => {
  const detected = detectDataSources();
  guildsCache = null;
  guildsCacheTime = 0;
  return res.json({ ok: true, detected });
});

app.get('/api/guilds', async (req, res) => {
  // Cache kontrol
  const now = Date.now();
  if (guildsCache && (now - guildsCacheTime) < CACHE_TTL) {
    console.log('[Guilds] Cache kullanılıyor');
    return res.json(guildsCache);
  }
  
  try {
    console.log('[Guilds] SQL + FindCord entegrasyonu başlıyor...');
    const guildsMap = new Map();
    
    // 1. SQL'den sunucuları çek (hızlı)
    for (const sqlPath of SQL_PATHS) {
      if (!fs.existsSync(sqlPath)) continue;
      try {
        const rs = fs.createReadStream(sqlPath, { encoding: 'utf8' });
        const rl = readline.createInterface({ input: rs, crlfDelay: Infinity });
        
        for await (const line of rl) {
          if (line.length > 10000) continue;

          // Kullanıcı ID: tuple başındaki 17-20 haneli değer
          const userIdMatch = line.match(/\(\s*(\d{17,20})\s*,/);
          const userId = userIdMatch?.[1];
          if (!userId || userId.startsWith('7656119')) continue;

          // Guild listeleri genelde bracket array olarak satır içinde geçiyor: [id,id,...]
          const lists = [...line.matchAll(/\[(\d{10,30}(?:,\d{10,30})*)\]/g)].map(m => m[1]);
          if (!lists.length) continue;

          // En olası guild listesi: içinde 17-20 haneli ID sayısı en fazla olan liste
          let bestIds = [];
          for (const raw of lists) {
            const ids = raw.split(',').map(s => s.trim()).filter(s => /^\d{17,20}$/.test(s) && !s.startsWith('7656119'));
            if (ids.length > bestIds.length) bestIds = ids;
          }
          if (!bestIds.length) continue;

          for (const gid of bestIds) {
            if (guildsMap.size >= 500 && !guildsMap.has(gid)) continue;
            const existing = guildsMap.get(gid);
            if (existing) {
              existing.member_count++;
              if (existing.sample_member_ids.length < 12 && !existing.sample_member_ids.includes(userId)) {
                existing.sample_member_ids.push(userId);
              }
            } else {
              guildsMap.set(gid, {
                id: gid,
                name: null,
                member_count: 1,
                source: path.basename(sqlPath),
                sample_member_ids: [userId]
              });
            }
          }
        }
        rl.close();
      } catch (err) {
        console.error(`[Guilds] SQL Hata ${sqlPath}:`, err.message);
      }
    }
    
    // 2. TÜM sunucuları zenginleştir (en popülerlerden başlayarak)
    let guilds = Array.from(guildsMap.values());
    guilds.sort((a, b) => b.member_count - a.member_count);

    // İlk 30 sunucuyu FindCord ile zenginleştir (daha fazla sunucu için)
    const findCordLimit = Math.min(30, guilds.length);
    const findCordTargets = guilds.slice(0, findCordLimit);

    console.log(`[Guilds] FindCord zenginleştirme: ${findCordTargets.length} sunucu için denenecek`);

    for (const guild of findCordTargets) {
      try {
        const sampleId = guild.sample_member_ids?.[0] || guild.id;
        const fcData = await getFindCordData(sampleId);

        if (fcData?.Guilds?.length > 0) {
          const matchingGuild = fcData.Guilds.find(g =>
            g.GuildId === guild.id || g.id === guild.id
          );

          if (matchingGuild) {
            guild.name = matchingGuild.GuildName || matchingGuild.name;
            guild.icon = matchingGuild.GuildIcon || matchingGuild.icon;
            guild.banner = matchingGuild.GuildBanner || matchingGuild.banner;
            guild.findcord_enriched = true;
          }
        }
      } catch (err) {
        console.log(`[Guilds] FindCord hata ${guild.id}:`, err.message);
      }
    }

    // 3. BOT TOKEN GEREKTİRMEYEN isim çözümleme (Widget API + Disboard)
    // Sadece en popüler 50 isimsiz sunucu için dene (timeout önleme)
    const namelessGuilds = guilds.filter(g => !g.name).slice(0, 50);
    console.log(`[Guilds] İsim çözümleme: ${namelessGuilds.length} isimsiz sunucu için deneniyor (top 50)...`);

    if (namelessGuilds.length > 0) {
      const resolvedResults = await batchResolveGuildNames(namelessGuilds);

      for (const result of resolvedResults) {
        if (result.status === 'fulfilled' && result.value?.name) {
          const guild = guilds.find(g => g.id === result.value.id);
          if (guild) {
            guild.name = result.value.name;
            guild.name_source = result.value.source;
            console.log(`[Guilds] ✅ İsim bulundu: ${guild.id} = "${result.value.name}" (${result.value.source})`);
          }
        }
      }
    }

    // Tüm sunucular için cache'de varsa isimleri uygula (ikinci kontrol)
    for (const guild of guilds) {
      if (!guild.name && guildNamesCache.has(guild.id)) {
        guild.name = guildNamesCache.get(guild.id);
        guild.name_source = 'cache';
      }
    }
    
    // 4. Discord CDN URL'leri oluştur
    for (const guild of guilds) {
      // Icon URL oluştur
      if (!guild.icon_url && guild.icon) {
        if (guild.icon.startsWith('http')) {
          guild.icon_url = guild.icon;
        } else {
          const ext = guild.icon.startsWith('a_') ? 'gif' : 'png';
          guild.icon_url = `https://cdn.discordapp.com/icons/${guild.id}/${guild.icon}.${ext}?size=128`;
        }
      }
      // Banner URL oluştur
      if (!guild.banner_url && guild.banner) {
        if (guild.banner.startsWith('http')) {
          guild.banner_url = guild.banner;
        } else {
          guild.banner_url = `https://cdn.discordapp.com/banners/${guild.id}/${guild.banner}.png?size=512`;
        }
      }
    }
    
    const result = {
      count: guilds.length,
      guilds: guilds.slice(0, 50),
      findcord_rate_limited: Date.now() < findCordRateLimitedUntil,
      findcord_retry_after_ms: Math.max(0, findCordRateLimitedUntil - Date.now())
    };
    
    // Cache'e kaydet
    guildsCache = result;
    guildsCacheTime = now;
    
    // Cevap dön
    return res.json(result);
    
  } catch (err) {
    console.error('[Guilds] Genel hata:', err);
    if (guildsCache) {
      return res.json({ ...guildsCache, cached: true, error: err.message });
    }
    return res.status(500).json({ error: 'internal_error', message: err.message });
  }
});

// İstatistikler
app.get('/api/stats', (req, res) => {
  let txtCount = 0, sqlCounts = {};
  try {
    if (fs.existsSync(TXT_PATH)) {
      const content = fs.readFileSync(TXT_PATH, 'utf8');
      const obj = safeJsonParse(content);
      txtCount = Array.isArray(obj?.users) ? obj.users.length : 0;
    }
  } catch { /* ignore */ }
  for (const p of SQL_PATHS) {
    try {
      if (!fs.existsSync(p)) { sqlCounts[path.basename(p)] = 0; continue; }
      const content = fs.readFileSync(p, 'utf8');
      const inserts = content.match(/INSERT INTO/gi);
      sqlCounts[path.basename(p)] = inserts ? inserts.length : 0;
    } catch { sqlCounts[path.basename(p)] = 0; }
  }
  res.json({ txt_records: txtCount, sql_tables: sqlCounts, total_sources: 1 + SQL_PATHS.length });
});

// 📥 MANUEL VERİ GİRİŞİ ENDPOINT
app.post('/api/manual-entry', async (req, res) => {
  try {
    const { type, discord_id, username, email, ip } = req.body || {};
    
    if (type === 'discord_info') {
      // Discord ID + bilgi kaydet
      if (!discord_id || !/^\d{17,20}$/.test(discord_id)) {
        return res.status(400).json({ ok: false, error: 'invalid_discord_id' });
      }
      
      // TXT dosyasına kaydet
      let data = { users: [] };
      try {
        if (fs.existsSync(TXT_PATH)) {
          const content = await fs.promises.readFile(TXT_PATH, 'utf8');
          data = safeJsonParse(content) || { users: [] };
        }
      } catch { /* ignore */ }
      
      if (!Array.isArray(data.users)) data.users = [];
      
      // Aynı ID var mı kontrol et
      const existingIndex = data.users.findIndex(u => String(u.discord_id) === String(discord_id));
      
      const newUser = {
        discord_id: String(discord_id),
        username: username || null,
        email: email || null,
        registration_ip: ip || null,
        last_ip: ip || null,
        source: 'manual_entry',
        added_at: new Date().toISOString()
      };
      
      if (existingIndex >= 0) {
        // Güncelle
        data.users[existingIndex] = { ...data.users[existingIndex], ...newUser };
      } else {
        // Yeni ekle
        data.users.push(newUser);
      }
      
      await fs.promises.writeFile(TXT_PATH, JSON.stringify(data, null, 2));
      console.log(`[Manual Entry] Discord ID kaydedildi: ${discord_id}`);
      
      return res.json({ ok: true, message: 'Veri kaydedildi' });
      
    } else if (type === 'email') {
      // Sadece email kaydet
      if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
        return res.status(400).json({ ok: false, error: 'invalid_email' });
      }
      
      // Burada email'i kaydedebiliriz ama Discord ID olmadan TXT yapısına uymuyor
      // Şimdilik basit bir kayıt yapacağız
      let data = { emails: [] };
      const emailPath = TXT_PATH.replace('.txt', '_emails.txt');
      
      try {
        if (fs.existsSync(emailPath)) {
          const content = await fs.promises.readFile(emailPath, 'utf8');
          data = safeJsonParse(content) || { emails: [] };
        }
      } catch { /* ignore */ }
      
      if (!Array.isArray(data.emails)) data.emails = [];
      
      if (!data.emails.includes(email)) {
        data.emails.push(email);
        await fs.promises.writeFile(emailPath, JSON.stringify(data, null, 2));
      }
      
      console.log(`[Manual Entry] Email kaydedildi: ${email}`);
      return res.json({ ok: true, message: 'Email kaydedildi' });
    }
    
    return res.status(400).json({ ok: false, error: 'invalid_type' });
    
  } catch (err) {
    console.error('[Manual Entry] Hata:', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// 📤 SQL DOSYA UPLOAD ENDPOINT (Railway için)
app.post('/api/upload-sql', express.raw({ type: '*/*', limit: '500mb' }), async (req, res) => {
  try {
    const filename = req.query.filename || 'uploaded.sql';
    if (!filename.endsWith('.sql') && !filename.endsWith('.txt')) {
      return res.status(400).json({ ok: false, error: 'Sadece .sql ve .txt dosyaları' });
    }
    
    const filepath = path.join(DATA_DIR, filename);
    fs.writeFileSync(filepath, req.body);
    
    console.log(`[Upload] ${filename} yüklendi: ${(req.body.length / 1024 / 1024).toFixed(2)} MB`);
    
    // SQL_PATHS'i güncelle
    detectDataSources();
    
    res.json({ ok: true, message: `${filename} yüklendi`, size: req.body.length });
  } catch (err) {
    console.error('[Upload] Hata:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// 📧 MAIL GÖNDERME ENDPOINT
app.post('/api/send-mail', async (req, res) => {
  try {
    const { to, subject, body } = req.body || {};
    if (!to || !subject || !body) {
      return res.status(400).json({ ok: false, error: 'missing_fields', message: 'to, subject ve body zorunlu' });
    }
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(to)) {
      return res.status(400).json({ ok: false, error: 'invalid_email', message: 'Geçersiz email adresi' });
    }

    const smtpHost = process.env.SMTP_HOST;
    const smtpPort = parseInt(process.env.SMTP_PORT || '587');
    const smtpUser = process.env.SMTP_USER;
    const smtpPass = process.env.SMTP_PASS;

    if (!smtpHost || !smtpUser || !smtpPass) {
      return res.status(503).json({ ok: false, error: 'smtp_not_configured', message: 'SMTP ayarları yapılandırılmamış. SMTP_HOST, SMTP_USER, SMTP_PASS env değişkenlerini ayarlayın.' });
    }

    const transporter = nodemailer.createTransport({
      host: smtpHost,
      port: smtpPort,
      secure: smtpPort === 465,
      auth: { user: smtpUser, pass: smtpPass }
    });

    await transporter.sendMail({
      from: `"Zagros OSINT" <${smtpUser}>`,
      to,
      subject,
      text: body,
      html: `<pre style="font-family:monospace">${body.replace(/</g,'&lt;').replace(/>/g,'&gt;')}</pre>`
    });

    console.log(`[Mail] Gönderildi: ${to} - ${subject}`);

    // Discord'a log
    await sendDiscordLog({
      title: '📧 Mail Gönderildi',
      color: 0x00b4d8,
      fields: [
        { name: 'Alıcı', value: to, inline: true },
        { name: 'Konu', value: subject, inline: true }
      ]
    });

    return res.json({ ok: true, message: 'Mail başarıyla gönderildi' });
  } catch (err) {
    console.error('[Mail] Hata:', err.message);
    return res.status(500).json({ ok: false, error: 'send_failed', message: err.message });
  }
});

// Discord'a embed log gönder (genel yardımcı)
async function sendDiscordLog(embed) {
  if (!DISCORD_WEBHOOK_URL) return;
  try {
    await axios.post(DISCORD_WEBHOOK_URL, {
      embeds: [{ ...embed, timestamp: new Date().toISOString(), footer: { text: 'Zagros OSINT' } }]
    }, { timeout: 5000 });
  } catch (err) {
    console.log('[Discord Log] Hata:', err.message);
  }
}

// Global error handler
app.use((err, req, res, next) => {
  console.error('[Global Error]', err.message);
  res.status(500).json({ error: 'internal_error', message: 'Sunucu hatası oluştu' });
});

app.use(express.static(path.join(__dirname, 'public')));

app.listen(APP_PORT, APP_HOST, () => {
  console.log(`zagros running at http://${APP_HOST}:${APP_PORT}`);
});
