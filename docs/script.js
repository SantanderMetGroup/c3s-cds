const DATA_DIR = 'data';
const HEATMAP_DIR = 'img/heatmaps';
const VALIDATION_DIR = 'img/validations';

const DATASET_INFO = {
  'derived-era5-single-levels-daily-statistics': { label: 'ERA5 Daily Statistics', type: 'Reanalysis', period: '1940-2024' },
  'derived-utci-historical': { label: 'UTCI Historical', type: 'Reanalysis', period: '2000-2020' },
  'insitu-gridded-observations-europe': { label: 'E-OBS In-situ Gridded', type: 'Observation', period: '1950-2024' },
  'reanalysis-cerra-land': { label: 'CERRA Land', type: 'Reanalysis', period: '1985-2025' },
  'reanalysis-cerra-single-levels': { label: 'CERRA Single Levels', type: 'Reanalysis', period: '1988-2017' },
  'reanalysis-era5-single-levels': { label: 'ERA5 Single Levels', type: 'Reanalysis', period: '1940-2024' },
  'satellite-sea-ice-concentration_nh': { label: 'Sea Ice Concentration (NH)', type: 'Satellite', period: '1978-2025' },
  'satellite-sea-ice-concentration_sh': { label: 'Sea Ice Concentration (SH)', type: 'Satellite', period: '1978-2025' },
  'satellite-sea-level-global': { label: 'Sea Level Global', type: 'Satellite', period: '1993-2023' },
  'satellite-surface-radiation-budget': { label: 'Surface Radiation Budget', type: 'Satellite', period: '1979-2025' },
};

const VALIDATION_FOLDER_MAP = {
  'satellite-sea-ice-concentration_nh': 'satellite-sea-ice-concentration',
  'satellite-sea-ice-concentration_sh': 'satellite-sea-ice-concentration',
};

function getStatus(row) {
  const start = row.start_file_exists && row.start_file_exists.toLowerCase() === 'true';
  const end = row.final_file_exists && row.final_file_exists.toLowerCase() === 'true';
  if (start && end) return 'complete';
  if (start || end) return 'partial';
  return 'missing';
}

function formatDate(d) {
  if (!d || d === 'nan' || d === '') return '—';
  const s = String(d);
  if (s.length === 8) return `${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}`;
  if (s.length === 6) return `${s.slice(0,4)}-${s.slice(4,6)}`;
  if (s.length === 4) return s;
  return s;
}

function datasetHeatmapPath(name) {
  return `${HEATMAP_DIR}/${name}_catalogue.png`;
}

function datasetValidationDir(name) {
  return VALIDATION_FOLDER_MAP[name] || name;
}

function escapeHtml(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

async function loadCSV(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`Failed to load ${path}`);
  const text = await res.text();
  const lines = text.trim().split('\n');
  const headers = parseCSVLine(lines[0]);
  const data = [];
  for (let i = 1; i < lines.length; i++) {
    const vals = parseCSVLine(lines[i]);
    if (vals.length === 0) continue;
    const row = {};
    headers.forEach((h, idx) => { row[h] = (vals[idx] || '').trim(); });
    data.push(row);
  }
  return data;
}

function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') { current += '"'; i++; }
        else { inQuotes = false; }
      } else { current += ch; }
    } else {
      if (ch === '"') { inQuotes = true; }
      else if (ch === ',') { result.push(current); current = ''; }
      else { current += ch; }
    }
  }
  result.push(current);
  return result;
}

function setupLightbox() {
  const lb = document.getElementById('lightbox');
  const lbImg = document.getElementById('lightbox-img');
  const lbClose = document.getElementById('lightbox-close');
  if (!lb) return;
  document.addEventListener('click', e => {
    const img = e.target.closest('.gallery-item img, .thumb, .detail-heatmap');
    if (!img) return;
    lbImg.src = img.src;
    lb.classList.add('active');
  });
  lb.addEventListener('click', () => lb.classList.remove('active'));
  if (lbClose) lbClose.addEventListener('click', () => lb.classList.remove('active'));
  document.addEventListener('keydown', e => {
    if (e.key === 'Escape' && lb.classList.contains('active')) lb.classList.remove('active');
  });
}

function showLoading(id) {
  const el = document.getElementById(id);
  if (el) el.innerHTML = '<div class="loading">Loading data...</div>';
}

function showError(id, msg) {
  const el = document.getElementById(id);
  if (el) el.innerHTML = `<div class="empty-state">${escapeHtml(msg)}</div>`;
}
