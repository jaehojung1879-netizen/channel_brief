// ============== GIS (gis.html) ==============
const DATA_BASE = './data';
const LOGO_BASE = './assets/bank-logos';

const BANK_META = {
  '신한':   { cls: 'shinhan', logo: 'shinhan.png', color: '#0046FF' },
  'KB국민': { cls: 'kb',      logo: 'kb.png',      color: '#FFBC00' },
  '하나':   { cls: 'hana',    logo: 'hana.png',    color: '#008C95' },
  '우리':   { cls: 'woori',   logo: 'woori.png',   color: '#0067AC' },
};
const BANK_ORDER = ['신한', 'KB국민', '하나', '우리'];

// 신한은행 본점 (서울 중구 세종대로9길 20)
const SHINHAN_HQ = { lat: 37.5654, lng: 126.9826, level: 4 };

// '365', '주차장' 등 무인채널/부속시설 키워드는 클라이언트에서도 한 번 더 거른다.
// (서버측 fetcher 도 동일 키워드로 제외하지만, 직전 데이터에 잔존할 수 있어 이중 가드)
const NAME_BLOCKLIST_RE = /(365|주\s*차\s*장|ATM|자동화\s*(?:코너|기기)|디지털\s*라운지|디라운지|키오스크|무인\s*(?:점포|창구)|환전\s*소)/i;

// 광역시·도 prefix → 표준 이름
const REGION_PREFIX = [
  ['서울특별시', '서울'], ['서울', '서울'],
  ['부산광역시', '부산'], ['부산', '부산'],
  ['대구광역시', '대구'], ['대구', '대구'],
  ['인천광역시', '인천'], ['인천', '인천'],
  ['광주광역시', '광주'], ['광주', '광주'],
  ['대전광역시', '대전'], ['대전', '대전'],
  ['울산광역시', '울산'], ['울산', '울산'],
  ['세종특별자치시', '세종'], ['세종', '세종'],
  ['경기도', '경기'], ['경기', '경기'],
  ['강원특별자치도', '강원'], ['강원도', '강원'], ['강원', '강원'],
  ['충청북도', '충북'], ['충북', '충북'],
  ['충청남도', '충남'], ['충남', '충남'],
  ['전북특별자치도', '전북'], ['전라북도', '전북'], ['전북', '전북'],
  ['전라남도', '전남'], ['전남', '전남'],
  ['경상북도', '경북'], ['경북', '경북'],
  ['경상남도', '경남'], ['경남', '경남'],
  ['제주특별자치도', '제주'], ['제주도', '제주'], ['제주', '제주'],
];

const STATE = {
  branches: [],
  filter: new Set(BANK_ORDER),
  logoGroup: 'all',          // 'all' | 'shinhan' | 'others'
  markers: [],
  map: null,
  infoWindow: null,
  regionStats: null,
  mobileMq: window.matchMedia('(max-width: 960px)'),
  // 유동인구 오버레이
  flowVisible: false,
  flowOverlays: [],
  // 영업점 단위 점수 캐시 (key = b.id 또는 idx, value = { branchScore, localPct, nearby, radiusKm })
  branchScores: new Map(),
};

// 수도권 + 광역시 + 세종 = 1km, 그 외 시·도 = 2km.
const METRO_SIDOS = new Set(['서울', '경기', '인천', '부산', '대구', '광주', '대전', '울산', '세종']);

// 서울 25 자치구 중심좌표 (대략값) — 유동인구 원형 오버레이 위치용.
const SEOUL_DISTRICT_CENTROIDS = {
  '강남구': { lat: 37.5172, lng: 127.0473 },
  '강동구': { lat: 37.5301, lng: 127.1238 },
  '강북구': { lat: 37.6396, lng: 127.0257 },
  '강서구': { lat: 37.5509, lng: 126.8495 },
  '관악구': { lat: 37.4784, lng: 126.9516 },
  '광진구': { lat: 37.5384, lng: 127.0822 },
  '구로구': { lat: 37.4954, lng: 126.8874 },
  '금천구': { lat: 37.4566, lng: 126.8954 },
  '노원구': { lat: 37.6542, lng: 127.0568 },
  '도봉구': { lat: 37.6688, lng: 127.0471 },
  '동대문구': { lat: 37.5744, lng: 127.0395 },
  '동작구': { lat: 37.5124, lng: 126.9393 },
  '마포구': { lat: 37.5663, lng: 126.9019 },
  '서대문구': { lat: 37.5791, lng: 126.9368 },
  '서초구': { lat: 37.4836, lng: 127.0327 },
  '성동구': { lat: 37.5634, lng: 127.0371 },
  '성북구': { lat: 37.5894, lng: 127.0167 },
  '송파구': { lat: 37.5145, lng: 127.1059 },
  '양천구': { lat: 37.5170, lng: 126.8665 },
  '영등포구': { lat: 37.5264, lng: 126.8962 },
  '용산구': { lat: 37.5384, lng: 126.9654 },
  '은평구': { lat: 37.6027, lng: 126.9291 },
  '종로구': { lat: 37.5735, lng: 126.9788 },
  '중구':   { lat: 37.5641, lng: 126.9979 },
  '중랑구': { lat: 37.6066, lng: 127.0926 },
};

function escHtml(s) {
  return String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
function fmtKstNow() {
  const d = new Date();
  const pad = n => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
}
function regionFromAddress(addr) {
  if (!addr) return '기타';
  for (const [prefix, label] of REGION_PREFIX) {
    if (addr.startsWith(prefix)) return label;
  }
  return '기타';
}

// kakao address 첫 두 토큰 → KOSIS / R-ONE 매핑용 표준 (시도, 시군구) key
function regionKeyFromAddress(addr) {
  if (!addr) return '';
  let sido = '';
  let rest = addr;
  for (const [prefix, label] of REGION_PREFIX) {
    if (addr.startsWith(prefix)) {
      sido = label;
      rest = addr.slice(prefix.length).trim();
      break;
    }
  }
  if (!sido) return '';
  const parts = rest.split(/\s+/).filter(Boolean);
  if (parts.length === 0) return sido;
  let sigungu = parts[0];
  if (parts.length >= 2 && parts[1].endsWith('구') && sigungu.endsWith('시')) {
    sigungu = `${sigungu} ${parts[1]}`;
  }
  return `${sido} ${sigungu}`;
}

function sidoFromAddress(addr) {
  if (!addr) return '';
  for (const [prefix, label] of REGION_PREFIX) {
    if (addr.startsWith(prefix)) return label;
  }
  return '';
}

function haversineKm(lat1, lng1, lat2, lng2) {
  const R = 6371;
  const toRad = d => d * Math.PI / 180;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a = Math.sin(dLat / 2) ** 2
    + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}

function radiusKmFor(branch) {
  const sido = sidoFromAddress(branch.address || branch.road_address || '');
  return METRO_SIDOS.has(sido) ? 1.0 : 2.0;
}

// 모든 영업점에 대해: 반경 내 (4 대銀) 점포수 → log 백분위 → 행정구 점수와 가중평균.
function computeBranchScores() {
  STATE.branchScores = new Map();
  const all = STATE.branches.filter(b => Number.isFinite(b.lat) && Number.isFinite(b.lng));
  if (all.length === 0) return;

  const nearby = new Array(all.length).fill(0);
  const radii = new Array(all.length);
  for (let i = 0; i < all.length; i++) radii[i] = radiusKmFor(all[i]);

  // O(N^2) 이지만 N≈수천이고 1 회만 계산 (필터 변경시에는 재계산 X — radius 는 영업점 위치 기반이므로 고정).
  for (let i = 0; i < all.length; i++) {
    const a = all[i]; const r = radii[i];
    for (let j = i + 1; j < all.length; j++) {
      const b = all[j];
      const d = haversineKm(a.lat, a.lng, b.lat, b.lng);
      if (d <= r) nearby[i]++;
      if (d <= radii[j]) nearby[j]++;
    }
  }

  // log 백분위 (양수만 고려; 0 이면 최저).
  const idxValues = [];
  for (let i = 0; i < nearby.length; i++) {
    if (nearby[i] > 0) idxValues.push([i, Math.log(nearby[i])]);
  }
  idxValues.sort((p, q) => p[1] - q[1]);
  const pcts = new Map();
  const n = idxValues.length;
  for (let order = 0; order < n; order++) {
    pcts.set(idxValues[order][0], (order / Math.max(n - 1, 1)) * 100);
  }

  for (let i = 0; i < all.length; i++) {
    const b = all[i];
    const localPct = pcts.has(i) ? pcts.get(i) : 0;
    const districtScore = districtScoreFor(b);
    let combined;
    if (districtScore != null) {
      combined = 0.5 * districtScore + 0.5 * localPct;
    } else {
      combined = localPct;
    }
    STATE.branchScores.set(b, {
      branchScore: Math.round(combined * 10) / 10,
      localPct: Math.round(localPct * 10) / 10,
      districtScore: districtScore != null ? Math.round(districtScore * 10) / 10 : null,
      nearby: nearby[i],
      radiusKm: radii[i],
    });
  }
}

function districtScoreFor(branch) {
  if (!STATE.regionStats || !STATE.regionStats.regions) return null;
  const key = regionKeyFromAddress(branch.address || branch.road_address || '');
  const entry = key && STATE.regionStats.regions[key];
  const v = entry && entry.location_score && entry.location_score.value;
  return (typeof v === 'number') ? v : null;
}

async function loadJSON(path) {
  try {
    const r = await fetch(`${DATA_BASE}/${path}?t=${Date.now()}`);
    if (!r.ok) throw new Error(`${r.status}`);
    return await r.json();
  } catch (e) {
    console.warn(`[GIS] load fail: ${path}`, e);
    return null;
  }
}

function showOverlay(title, msg, isError = false) {
  const overlay = document.getElementById('map-overlay');
  if (!overlay) return;
  overlay.classList.remove('hidden');
  const card = overlay.querySelector('.overlay-card');
  if (card) card.classList.toggle('error', !!isError);
  const t = overlay.querySelector('.overlay-title');
  const m = document.getElementById('overlay-msg');
  if (t) t.textContent = title;
  if (m) m.textContent = msg;
}
function hideOverlay() {
  const overlay = document.getElementById('map-overlay');
  if (overlay) overlay.classList.add('hidden');
}



function relocateMobileControls() {
  const mobileWrap = document.getElementById('mobile-map-controls');
  const sidebar = document.querySelector('.gis-side');
  const searchBlock = document.getElementById('search-block');
  const filterBlock = document.getElementById('filter-block');
  if (!mobileWrap || !sidebar || !searchBlock || !filterBlock) return;

  const toMobile = STATE.mobileMq.matches;
  if (toMobile) {
    mobileWrap.appendChild(searchBlock);
    mobileWrap.appendChild(filterBlock);
  } else {
    sidebar.prepend(filterBlock);
    sidebar.prepend(searchBlock);
  }
}

// ============== Sidebar ==============
function renderBankToggles() {
  const el = document.getElementById('bank-toggle-list');
  if (!el) return;
  const counts = {};
  STATE.branches.forEach(b => { counts[b.bank] = (counts[b.bank] || 0) + 1; });
  el.innerHTML = BANK_ORDER.map(name => {
    const meta = BANK_META[name] || {};
    const total = counts[name] || 0;
    const off = !STATE.filter.has(name);
    return `
      <div class="bank-toggle ${meta.cls}${off ? ' off' : ''}" data-bank="${escHtml(name)}">
        <span class="swatch"></span>
        <span class="name">${escHtml(name)}은행</span>
        <span class="count">${total.toLocaleString()}</span>
      </div>
    `;
  }).join('');
  el.querySelectorAll('.bank-toggle').forEach(node => {
    node.addEventListener('click', () => {
      const name = node.dataset.bank;
      if (STATE.filter.has(name)) STATE.filter.delete(name);
      else STATE.filter.add(name);
      node.classList.toggle('off');
      renderMarkers();
    });
  });
}

function bindLogoGroupToggle() {
  const wrap = document.getElementById('logo-group-toggle');
  if (!wrap) return;
  wrap.addEventListener('change', (ev) => {
    const t = ev.target;
    if (!t || t.name !== 'logoGroup') return;
    STATE.logoGroup = t.value;
    syncBankFilterFromLogoGroup();
    renderBankToggles();
    renderMarkers();
  });
}

function syncBankFilterFromLogoGroup() {
  if (STATE.logoGroup === 'shinhan') {
    STATE.filter = new Set(['신한']);
  } else if (STATE.logoGroup === 'others') {
    STATE.filter = new Set(BANK_ORDER.filter(n => n !== '신한'));
  } else {
    STATE.filter = new Set(BANK_ORDER);
  }
}

function renderStats(meta) {
  document.getElementById('stat-total').textContent = STATE.branches.length.toLocaleString();
  document.getElementById('stat-asof').textContent = meta?.as_of
    ? meta.as_of.replace('T', ' ').slice(0, 16)
    : '—';
  document.getElementById('meta-source').textContent = `SOURCE: ${meta?.source || '—'}`;
  document.getElementById('meta-updated').textContent = `UPDATED: ${meta?.as_of || '—'}`;
}

function renderRegionBars() {
  const el = document.getElementById('region-bars');
  if (!el) return;
  const counts = new Map();
  STATE.branches.forEach(b => {
    const reg = regionFromAddress(b.address || b.road_address || '');
    counts.set(reg, (counts.get(reg) || 0) + 1);
  });
  if (counts.size === 0) {
    el.innerHTML = `<div class="loading">데이터 수집 대기 중</div>`;
    return;
  }
  const entries = Array.from(counts.entries()).sort((a, b) => b[1] - a[1]);
  const max = entries[0][1] || 1;
  el.innerHTML = entries.map(([reg, n]) => {
    const pct = (n / max) * 100;
    return `
      <div class="region-row">
        <span class="reg">${escHtml(reg)}</span>
        <div class="bar"><div style="width:${pct.toFixed(1)}%;"></div></div>
        <span class="num">${n.toLocaleString()}</span>
      </div>
    `;
  }).join('');
}

// ============== Branch search (신한 only) ==============
function bindBranchSearch() {
  const input = document.getElementById('branch-search');
  const out = document.getElementById('branch-search-results');
  if (!input || !out) return;

  let activeIdx = -1;
  let lastMatches = [];

  const render = (matches) => {
    lastMatches = matches;
    activeIdx = -1;
    if (matches.length === 0) {
      out.hidden = true;
      out.innerHTML = '';
      return;
    }
    out.innerHTML = matches.map((b, i) => `
      <button type="button" class="search-row" data-idx="${i}">
        <span class="bn">${escHtml(b.name)}</span>
        <span class="ad">${escHtml(b.road_address || b.address || '')}</span>
      </button>
    `).join('');
    out.hidden = false;
  };

  const onPick = (b) => {
    if (!b || !STATE.map) return;
    out.hidden = true;
    input.value = b.name;
    if (Number.isFinite(b.lat) && Number.isFinite(b.lng)) {
      const pos = new kakao.maps.LatLng(b.lat, b.lng);
      STATE.map.setLevel(3);
      STATE.map.setCenter(pos);
      openInfo(b, pos);
    }
  };

  input.addEventListener('input', () => {
    const q = input.value.trim().toLowerCase();
    if (!q) { render([]); return; }
    const pool = STATE.branches.filter(b => b.bank === '신한');
    const matches = pool.filter(b =>
      (b.name || '').toLowerCase().includes(q) ||
      (b.road_address || '').toLowerCase().includes(q) ||
      (b.address || '').toLowerCase().includes(q)
    ).slice(0, 12);
    render(matches);
  });

  input.addEventListener('keydown', (ev) => {
    if (out.hidden || lastMatches.length === 0) return;
    if (ev.key === 'ArrowDown') {
      ev.preventDefault();
      activeIdx = Math.min(activeIdx + 1, lastMatches.length - 1);
    } else if (ev.key === 'ArrowUp') {
      ev.preventDefault();
      activeIdx = Math.max(activeIdx - 1, 0);
    } else if (ev.key === 'Enter') {
      ev.preventDefault();
      const pick = activeIdx >= 0 ? lastMatches[activeIdx] : lastMatches[0];
      onPick(pick);
      return;
    } else if (ev.key === 'Escape') {
      out.hidden = true;
      return;
    } else {
      return;
    }
    out.querySelectorAll('.search-row').forEach((node, i) => {
      node.classList.toggle('active', i === activeIdx);
    });
  });

  out.addEventListener('mousedown', (ev) => {
    const row = ev.target.closest('.search-row');
    if (!row) return;
    ev.preventDefault();
    const i = Number(row.dataset.idx);
    if (Number.isFinite(i)) onPick(lastMatches[i]);
  });

  document.addEventListener('click', (ev) => {
    if (!out.contains(ev.target) && ev.target !== input) {
      out.hidden = true;
    }
  });
}

// ============== Kakao Map ==============
function buildMarkerNode(bank) {
  const meta = BANK_META[bank];
  if (!meta) return null;
  const node = document.createElement('div');
  node.className = `bank-marker ${meta.cls}`;
  node.style.background = '#fff';
  const img = document.createElement('img');
  img.src = `${LOGO_BASE}/${meta.logo}`;
  img.alt = bank;
  img.onerror = () => { img.style.display = 'none'; };
  node.appendChild(img);
  return node;
}

function clearMarkers() {
  STATE.markers.forEach(m => {
    if (m.overlay) m.overlay.setMap(null);
  });
  STATE.markers = [];
}

function regionStatsHtml(b) {
  if (!STATE.regionStats || !STATE.regionStats.regions) {
    return `<div class="rs-empty">통계청·한국부동산원·서울OpenAPI 자료는 다음 워크플로 실행 후 표시됩니다.</div>`;
  }
  const key = regionKeyFromAddress(b.address || b.road_address || '');
  const entry = key && STATE.regionStats.regions[key];
  if (!entry) {
    return `<div class="rs-empty">${escHtml(key || '지역 미상')} · 자료 미수집</div>`;
  }
  const rows = [];

  // 영업점 단위 점수 (행정구 점수 50% + 반경 내 점포밀도 50%) 를 최상단에 강조.
  const bs = STATE.branchScores.get(b);
  if (bs) {
    const v = Number(bs.branchScore).toFixed(1);
    rows.push(`<div class="rs-row rs-score"><span class="k">영업점 점수</span><span class="v">${v} / 100</span><span class="src">반경${bs.radiusKm}km</span></div>`);
    const dpart = bs.districtScore != null ? `행정구 ${bs.districtScore.toFixed(0)}p` : null;
    const lpart = `반경 ${bs.radiusKm}km ${bs.nearby}개·${bs.localPct.toFixed(0)}p`;
    const parts = [dpart, lpart].filter(Boolean);
    rows.push(`<div class="rs-row rs-score-breakdown"><span class="k"></span><span class="v">${escHtml(parts.join(' · '))}</span><span class="src"></span></div>`);
  }

  // 행정구(시·군·구) 점수 — 사업체수 45% / 소득 35% / 시장(인구·유동인구) 20%.
  const sc = entry.location_score;
  if (sc && sc.value != null) {
    const v = Number(sc.value).toFixed(1);
    rows.push(`<div class="rs-row"><span class="k">행정구 점수</span><span class="v">${v} / 100</span><span class="src">백분위</span></div>`);
    const c = sc.components || {};
    const parts = [];
    if (c.businesses_pct != null) parts.push(`사업체 ${Number(c.businesses_pct).toFixed(0)}p`);
    if (c.income_pct != null) parts.push(`소득 ${Number(c.income_pct).toFixed(0)}p`);
    if (c.market_pct != null) {
      const lbl = c.market_source === 'floating_population' ? '유동인구' : '인구';
      parts.push(`${lbl} ${Number(c.market_pct).toFixed(0)}p`);
    }
    if (c.branch_density_pct != null) parts.push(`점포밀도 ${Number(c.branch_density_pct).toFixed(0)}p`);
    if (parts.length) {
      rows.push(`<div class="rs-row rs-score-breakdown"><span class="k"></span><span class="v">${escHtml(parts.join(' · '))}</span><span class="src"></span></div>`);
    }
  }

  if (entry.businesses && entry.businesses.value != null) {
    const v = Number(entry.businesses.value).toLocaleString();
    const period = entry.businesses.period ? ` (${escHtml(entry.businesses.period)})` : '';
    rows.push(`<div class="rs-row"><span class="k">사업체수</span><span class="v">${v} 개${period}</span><span class="src">KOSIS</span></div>`);
  }
  if (entry.income && entry.income.value != null) {
    const raw = Number(entry.income.value);
    const v = Number.isFinite(raw) ? raw.toLocaleString(undefined, { maximumFractionDigits: 0 }) : '—';
    const unit = entry.income.unit ? ` ${escHtml(entry.income.unit)}` : '';
    const period = entry.income.period ? ` (${escHtml(entry.income.period)})` : '';
    const scope = entry.income.scope === 'sido' ? ' · 시·도 평균' : '';
    rows.push(`<div class="rs-row"><span class="k">평균 가구소득</span><span class="v">${v}${unit}${period}</span><span class="src">KOSIS${scope}</span></div>`);
  }
  if (entry.population && entry.population.value != null) {
    const v = Number(entry.population.value).toLocaleString();
    const period = entry.population.period ? ` (${escHtml(entry.population.period)})` : '';
    rows.push(`<div class="rs-row"><span class="k">인구</span><span class="v">${v} 명${period}</span><span class="src">KOSIS</span></div>`);
  }
  if (entry.floating_population && entry.floating_population.value != null) {
    const v = Number(entry.floating_population.value).toLocaleString(undefined, { maximumFractionDigits: 1 });
    const samples = entry.floating_population.samples ? ` · n=${entry.floating_population.samples.toLocaleString()}` : '';
    rows.push(`<div class="rs-row"><span class="k">유동인구</span><span class="v">${v} 명/측정${samples}</span><span class="src">서울 OpenAPI</span></div>`);
  }
  if (entry.branch_count != null) {
    rows.push(`<div class="rs-row"><span class="k">관내 4대銀 점포</span><span class="v">${entry.branch_count.toLocaleString()} 개</span><span class="src">Kakao</span></div>`);
  }
  if (entry.price_index && entry.price_index.value != null) {
    const v = Number(entry.price_index.value).toFixed(1);
    const period = entry.price_index.period ? ` (${escHtml(entry.price_index.period)})` : '';
    rows.push(`<div class="rs-row"><span class="k">매매가격지수</span><span class="v">${v}${period}</span><span class="src">R-ONE</span></div>`);
  }
  if (rows.length === 0) {
    return `<div class="rs-empty">${escHtml(key)} · 자료 미수집</div>`;
  }
  return `<div class="rs-block"><div class="rs-head">${escHtml(key)} · 입지 자료</div>${rows.join('')}</div>`;
}

function infoHtml(b) {
  const region = regionFromAddress(b.address || b.road_address || '');
  return `
    <div class="gis-info">
      <div class="bn">${escHtml(b.name)}</div>
      <div class="ad">${escHtml(b.road_address || b.address || '')}</div>
      ${b.phone ? `<div class="ph">${escHtml(b.phone)}</div>` : ''}
      <div class="ph">${escHtml(b.bank)} · ${escHtml(region)}</div>
      ${regionStatsHtml(b)}
    </div>
  `;
}

function isVisibleByFilter(b) {
  if (!STATE.filter.has(b.bank)) return false;
  if (NAME_BLOCKLIST_RE.test(b.name || '')) return false;
  return true;
}

function renderMarkers() {
  if (!STATE.map) return;
  clearMarkers();
  const visible = STATE.branches.filter(isVisibleByFilter);
  visible.forEach(b => {
    if (!Number.isFinite(b.lat) || !Number.isFinite(b.lng)) return;
    const pos = new kakao.maps.LatLng(b.lat, b.lng);
    const node = buildMarkerNode(b.bank);
    if (!node) return;
    node.addEventListener('click', (ev) => {
      ev.stopPropagation();
      openInfo(b, pos);
    });
    const overlay = new kakao.maps.CustomOverlay({
      position: pos,
      content: node,
      yAnchor: 1,
      xAnchor: 0.5,
      clickable: true,
    });
    overlay.setMap(STATE.map);
    STATE.markers.push({ overlay, branch: b, pos });
  });
}

function openInfo(b, pos) {
  if (!STATE.infoWindow) {
    STATE.infoWindow = new kakao.maps.InfoWindow({ removable: true, zIndex: 50 });
  }
  STATE.infoWindow.setContent(infoHtml(b));
  STATE.infoWindow.setPosition(pos);
  STATE.infoWindow.open(STATE.map);
  STATE.map.panTo(pos);
}

// ============== Kakao SDK loader ==============
function loadKakaoSdk(jsKey) {
  return new Promise((resolve, reject) => {
    if (window.kakao && window.kakao.maps) {
      resolve();
      return;
    }
    const s = document.createElement('script');
    s.async = true;
    s.src = `https://dapi.kakao.com/v2/maps/sdk.js?appkey=${encodeURIComponent(jsKey)}&autoload=false&libraries=services,clusterer`;
    s.onload = () => {
      if (window.kakao && window.kakao.maps) {
        kakao.maps.load(() => resolve());
      } else {
        reject(new Error('Kakao SDK 로딩 직후 kakao.maps 객체를 찾을 수 없습니다.'));
      }
    };
    s.onerror = () => reject(new Error('Kakao SDK 스크립트 로드 실패 (네트워크 / 도메인 등록 확인 필요).'));
    document.head.appendChild(s);
  });
}

function findShinhanHQ() {
  // '신한은행 본점' / '본점영업부' 등을 우선 매칭, 실패 시 좌표 fallback.
  const shinhan = STATE.branches.filter(b => b.bank === '신한');
  const hq = shinhan.find(b => /본점\s*영업부/.test(b.name || ''))
          || shinhan.find(b => /신한.*본점|본점.*신한/.test(b.name || ''))
          || shinhan.find(b => /본점/.test(b.name || ''));
  if (hq && Number.isFinite(hq.lat) && Number.isFinite(hq.lng)) {
    return { lat: hq.lat, lng: hq.lng };
  }
  return { lat: SHINHAN_HQ.lat, lng: SHINHAN_HQ.lng };
}

function initMap() {
  const container = document.getElementById('map');
  // 첫 진입: 신한은행 본점을 중심으로, 줌은 max(=1)에서 3단계 완화한 level 4.
  const hq = findShinhanHQ();
  STATE.map = new kakao.maps.Map(container, {
    center: new kakao.maps.LatLng(hq.lat, hq.lng),
    level: SHINHAN_HQ.level,
  });
  // 일부 안드로이드 태블릿 (예: 삼성 갤럭시 패드) 에서 기본값이 무시되는 사례가 있어 명시.
  if (typeof STATE.map.setDraggable === 'function') STATE.map.setDraggable(true);
  if (typeof STATE.map.setZoomable === 'function') STATE.map.setZoomable(true);
  const zoomCtrl = new kakao.maps.ZoomControl();
  STATE.map.addControl(zoomCtrl, kakao.maps.ControlPosition.TOPRIGHT);
  kakao.maps.event.addListener(STATE.map, 'click', () => {
    if (STATE.infoWindow) STATE.infoWindow.close();
  });
}

// ============== Seoul 유동인구 오버레이 ==============
function seoulFlowEntries() {
  const out = [];
  if (!STATE.regionStats || !STATE.regionStats.regions) return out;
  for (const [district, centroid] of Object.entries(SEOUL_DISTRICT_CENTROIDS)) {
    const entry = STATE.regionStats.regions[`서울 ${district}`] || null;
    const fp = entry && entry.floating_population;
    if (!fp || fp.value == null) continue;
    out.push({ district, centroid, value: fp.value, samples: fp.samples });
  }
  return out;
}

function flowSizePx(value, minVal, maxVal) {
  // 평균 visitor count 를 30~120px 범위로 매핑 (sqrt 스케일).
  const lo = 30, hi = 120;
  if (!(maxVal > minVal)) return (lo + hi) / 2;
  const t = Math.sqrt(Math.max(value - minVal, 0) / (maxVal - minVal));
  return lo + (hi - lo) * t;
}

function clearSeoulFlowOverlays() {
  STATE.flowOverlays.forEach(o => { try { o.setMap(null); } catch (_e) {} });
  STATE.flowOverlays = [];
}

function renderSeoulFlowOverlays() {
  clearSeoulFlowOverlays();
  if (!STATE.map || !STATE.flowVisible) return;
  const entries = seoulFlowEntries();
  if (entries.length === 0) return;

  const values = entries.map(e => e.value);
  const minV = Math.min(...values);
  const maxV = Math.max(...values);

  entries.forEach(e => {
    const size = flowSizePx(e.value, minV, maxV);
    const node = document.createElement('div');
    node.className = 'flow-circle';
    node.style.width = `${size}px`;
    node.style.height = `${size}px`;
    node.innerHTML = `<div class="flow-lbl"><span class="d">${escHtml(e.district)}</span><span class="v">${Number(e.value).toFixed(0)}명</span></div>`;
    const overlay = new kakao.maps.CustomOverlay({
      position: new kakao.maps.LatLng(e.centroid.lat, e.centroid.lng),
      content: node,
      yAnchor: 0.5,
      xAnchor: 0.5,
      clickable: false,
      zIndex: 0,
    });
    overlay.setMap(STATE.map);
    STATE.flowOverlays.push(overlay);
  });
}

// ============== 지도 위 범례 (은행 필터 + 유동인구 토글) ==============
function buildMapLegend() {
  const main = document.querySelector('.gis-main');
  if (!main) return;
  // 모바일에서는 sidebar 위쪽 mobile-map-controls 를 그대로 쓰므로 데스크탑 한정.
  if (STATE.mobileMq.matches) {
    const existing = document.getElementById('map-legend');
    if (existing) existing.remove();
    return;
  }
  let leg = document.getElementById('map-legend');
  if (!leg) {
    leg = document.createElement('div');
    leg.id = 'map-legend';
    leg.className = 'map-legend';
    main.appendChild(leg);
  }
  // 은행 필터 (로고 토글) + 유동인구 토글.
  leg.innerHTML = `
    <div class="legend-row legend-banks" id="legend-banks">
      ${BANK_ORDER.map(name => {
        const meta = BANK_META[name] || {};
        const off = !STATE.filter.has(name);
        return `<button type="button" class="legend-bank ${meta.cls}${off ? ' off' : ''}" data-bank="${escHtml(name)}" title="${escHtml(name)}은행">
          <img src="${LOGO_BASE}/${meta.logo}" alt="${escHtml(name)}" />
        </button>`;
      }).join('')}
    </div>
    <label class="legend-row legend-flow">
      <input type="checkbox" id="legend-flow-toggle" ${STATE.flowVisible ? 'checked' : ''} />
      <span class="dot"></span>
      <span class="lbl">서울 유동인구</span>
    </label>
  `;

  leg.querySelectorAll('.legend-bank').forEach(btn => {
    btn.addEventListener('click', () => {
      const name = btn.dataset.bank;
      if (STATE.filter.has(name)) STATE.filter.delete(name);
      else STATE.filter.add(name);
      btn.classList.toggle('off');
      // 사이드바 (있다면) 와 동기화.
      const sb = document.querySelector(`#bank-toggle-list .bank-toggle[data-bank="${name}"]`);
      if (sb) sb.classList.toggle('off');
      renderMarkers();
    });
  });
  const cb = leg.querySelector('#legend-flow-toggle');
  if (cb) cb.addEventListener('change', () => {
    STATE.flowVisible = !!cb.checked;
    renderSeoulFlowOverlays();
  });
}

// ============== Bootstrap ==============
async function bootstrap() {
  document.getElementById('today-date').textContent = fmtKstNow();
  bindLogoGroupToggle();
  relocateMobileControls();
  STATE.mobileMq.addEventListener('change', relocateMobileControls);

  const [config, branchesPayload, regionStats] = await Promise.all([
    loadJSON('kakao_config.json'),
    loadJSON('kakao_branches.json'),
    loadJSON('regional_stats.json'),
  ]);

  const meta = branchesPayload || {};
  const rawBranches = Array.isArray(meta.branches) ? meta.branches : [];
  // 직전 데이터에 잔존하는 '365' / '주차장' 등 무인채널/부속시설은 클라이언트에서도 제외.
  STATE.branches = rawBranches.filter(b =>
    BANK_META[b.bank] && !NAME_BLOCKLIST_RE.test(b.name || '')
  );
  STATE.regionStats = regionStats;

  renderStats(meta);
  renderBankToggles();
  renderRegionBars();
  bindBranchSearch();

  const jsKey = (config && config.jsKey) ? config.jsKey.trim() : '';
  if (!jsKey) {
    showOverlay(
      'Kakao JS 키 미설정',
      'GitHub Secrets 에 KAKAO_JS_KEY 를 등록한 뒤\n다음 daily-update 워크플로 실행 후 다시 열어주세요.\n(또는 Actions 탭에서 수동 실행)',
      true,
    );
    return;
  }

  try {
    await loadKakaoSdk(jsKey);
  } catch (e) {
    showOverlay('Kakao SDK 로드 실패', String(e?.message || e) + '\n도메인 화이트리스트 / 키 확인 필요.', true);
    return;
  }

  initMap();

  if (STATE.branches.length === 0) {
    const diag = meta.diagnostics || {};
    const lines = ['KAKAO_REST_API_KEY 등록 여부를 확인해 주세요.'];
    if (diag.rest_key_resolved === false) {
      const seen = diag.kakao_env_seen || {};
      const setNames = Object.keys(seen).filter(k => seen[k] === 'set');
      const emptyNames = Object.keys(seen).filter(k => seen[k] === 'empty');
      if (setNames.length) {
        lines.push(`현재 워크플로에서 인식된 KAKAO_* secret: ${setNames.join(', ')}`);
      } else {
        lines.push('현재 워크플로에서 인식된 KAKAO_* secret: 없음');
      }
      if (emptyNames.length) {
        lines.push(`empty 로 들어온 candidate: ${emptyNames.join(', ')}`);
      }
      if (diag.hint) lines.push(diag.hint);
    } else if (diag.note) {
      lines.push(diag.note);
    } else {
      lines.push('워크플로를 다시 실행하면 영업점 데이터가 지도에 반영됩니다.');
    }
    showOverlay('영업점 좌표 미수집', lines.join('\n'), true);
    return;
  }

  hideOverlay();
  computeBranchScores();
  renderMarkers();
  buildMapLegend();
  STATE.mobileMq.addEventListener('change', buildMapLegend);
}

document.addEventListener('DOMContentLoaded', bootstrap);
