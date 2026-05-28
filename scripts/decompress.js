/**
 * scripts/decompress.js
 *
 * /data klasöründeki (veya belirtilen dizindeki) .gz dosyalarını
 * decompress ederek aynı dizine orijinal adıyla kaydeder.
 *
 * Kullanım:
 *   node scripts/decompress.js [--dir /custom/path] [file1.gz file2.gz ...]
 *
 *   Argüman verilmezse DATA_DIR içindeki tüm .gz dosyaları işlenir.
 *
 * Ortam değişkenleri:
 *   RAILWAY_VOLUME_MOUNT_PATH  — Railway volume yolu (yoksa ./data kullanılır)
 */

import fs   from 'node:fs';
import path from 'node:path';
import { createGunzip }    from 'node:zlib';
import { pipeline }        from 'node:stream/promises';
import { fileURLToPath }   from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

// ─── Dizin yapılandırması ────────────────────────────────────────────────────
const DATA_DIR = process.env.RAILWAY_VOLUME_MOUNT_PATH
  ? process.env.RAILWAY_VOLUME_MOUNT_PATH
  : path.resolve(__dirname, '..', 'data');

// ─── Yardımcı: boyutu okunabilir formata çevir ──────────────────────────────
function humanSize(bytes) {
  if (bytes < 1024)       return `${bytes} B`;
  if (bytes < 1024 ** 2)  return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 ** 3)  return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
  return `${(bytes / 1024 ** 3).toFixed(2)} GB`;
}

// ─── Tek bir .gz dosyasını decompress et ────────────────────────────────────
async function decompressFile(gzPath, outPath) {
  const label = path.basename(gzPath);

  if (fs.existsSync(outPath)) {
    console.log(`[Decompress] ⏭  ${path.basename(outPath)} zaten mevcut, atlanıyor`);
    return { skipped: true };
  }

  console.log(`[Decompress] 📦 ${label} → ${path.basename(outPath)}`);

  const src  = fs.createReadStream(gzPath);
  const gunz = createGunzip();
  const dest = fs.createWriteStream(outPath);

  const startTime = Date.now();

  try {
    await pipeline(src, gunz, dest);
  } catch (err) {
    // Yarım kalan dosyayı temizle
    try { fs.unlinkSync(outPath); } catch { /* ignore */ }
    throw err;
  }

  const elapsed  = ((Date.now() - startTime) / 1000).toFixed(1);
  const outSize  = fs.statSync(outPath).size;
  console.log(`[Decompress] ✅ ${path.basename(outPath)} — ${humanSize(outSize)} (${elapsed}s)`);
  return { skipped: false, outSize };
}

// ─── Dizindeki tüm .gz dosyalarını bul ──────────────────────────────────────
function findGzFiles(dir) {
  try {
    return fs
      .readdirSync(dir, { withFileTypes: true })
      .filter((e) => e.isFile() && e.name.endsWith('.gz'))
      .map((e) => path.join(dir, e.name));
  } catch (err) {
    console.error(`[Decompress] ❌ Dizin okunamadı (${dir}): ${err.message}`);
    return [];
  }
}

// ─── CLI argümanlarını ayrıştır ──────────────────────────────────────────────
function parseArgs(argv) {
  const args   = argv.slice(2); // node + script adını atla
  let   dir    = DATA_DIR;
  const files  = [];

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--dir' && args[i + 1]) {
      dir = path.resolve(args[++i]);
    } else {
      files.push(path.resolve(args[i]));
    }
  }

  return { dir, files };
}

// ─── Ana akış ────────────────────────────────────────────────────────────────
async function main() {
  console.log('═══════════════════════════════════════════════════════');
  console.log('  Zagros Decompress — .gz → orijinal dosya');
  console.log(`  DATA_DIR : ${DATA_DIR}`);
  console.log('═══════════════════════════════════════════════════════\n');

  const { dir, files: cliFiles } = parseArgs(process.argv);

  // Hedef dizini oluştur
  fs.mkdirSync(dir, { recursive: true });

  // İşlenecek .gz dosyalarını belirle
  const gzFiles = cliFiles.length > 0
    ? cliFiles.filter((f) => {
        if (!f.endsWith('.gz')) {
          console.warn(`[Decompress] ⚠  ${path.basename(f)} .gz değil, atlanıyor`);
          return false;
        }
        if (!fs.existsSync(f)) {
          console.warn(`[Decompress] ⚠  ${f} bulunamadı, atlanıyor`);
          return false;
        }
        return true;
      })
    : findGzFiles(dir);

  if (gzFiles.length === 0) {
    console.log('[Decompress] ℹ  İşlenecek .gz dosyası bulunamadı.');
    return;
  }

  console.log(`[Decompress] ${gzFiles.length} dosya işlenecek\n`);

  let successCount = 0;
  let skipCount    = 0;
  let errorCount   = 0;

  for (const gzPath of gzFiles) {
    // Çıktı yolu: aynı dizin, .gz uzantısı kaldırılmış
    const outDir  = path.dirname(gzPath);
    const outName = path.basename(gzPath, '.gz');
    const outPath = path.join(outDir, outName);

    try {
      const result = await decompressFile(gzPath, outPath);
      if (result.skipped) skipCount++;
      else successCount++;
    } catch (err) {
      errorCount++;
      console.error(`[Decompress] ❌ ${path.basename(gzPath)}: ${err.message}`);
    }
  }

  console.log('\n═══════════════════════════════════════════════════════');
  console.log(`  Tamamlandı — ✅ ${successCount} başarılı, ⏭ ${skipCount} atlandı, ❌ ${errorCount} hata`);
  console.log('═══════════════════════════════════════════════════════');

  if (errorCount > 0) process.exit(1);
}

main().catch((err) => {
  console.error('[Fatal]', err);
  process.exit(1);
});
