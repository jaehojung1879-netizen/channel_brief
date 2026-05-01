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
  markers: [],
  map: null,
  infoWindow: null,
  mapBounds: null,
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

function infoHtml(b) {
  const region = regionFromAddress(b.address || b.road_address || '');
  const place = b.place_url ? `<a href="${escHtml(b.place_url)}" target="_blank" rel="noopener">Kakao Place →</a>` : '';
  return `
    <div class="gis-info">
      <div class="bn">${escHtml(b.name)}</div>
      <div class="ad">${escHtml(b.road_address || b.address || '')}</div>
      ${b.phone ? `<div class="ph">${escHtml(b.phone)}</div>` : ''}
      <div class="ph">${escHtml(b.bank)} · ${escHtml(region)}</div>
      ${place}
    </div>
  `;
}

function renderMarkers() {
  if (!STATE.map) return;
  clearMarkers();
  const visible = STATE.branches.filter(b => STATE.filter.has(b.bank));
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

function initMap() {
  const container = document.getElementById('map');
  STATE.map = new kakao.maps.Map(container, {
    center: new kakao.maps.LatLng(36.5, 127.8),
    level: 13,
  });
  // 표준 컨트롤
  const zoomCtrl = new kakao.maps.ZoomControl();
  STATE.map.addControl(zoomCtrl, kakao.maps.ControlPosition.TOPRIGHT);
  // 빈 영역 클릭 시 InfoWindow 닫기
  kakao.maps.event.addListener(STATE.map, 'click', () => {
    if (STATE.infoWindow) STATE.infoWindow.close();
  });
}

function fitToBranches() {
  if (!STATE.map || STATE.branches.length === 0) return;
  const bounds = new kakao.maps.LatLngBounds();
  let added = 0;
  STATE.branches.forEach(b => {
    if (Number.isFinite(b.lat) && Number.isFinite(b.lng)) {
      bounds.extend(new kakao.maps.LatLng(b.lat, b.lng));
      added++;
    }
  });
  if (added > 0) STATE.map.setBounds(bounds);
}

// ============== Bootstrap ==============
async function bootstrap() {
  document.getElementById('today-date').textContent = fmtKstNow();

  const [config, branchesPayload] = await Promise.all([
    loadJSON('kakao_config.json'),
    loadJSON('kakao_branches.json'),
  ]);

  const meta = branchesPayload || {};
  STATE.branches = Array.isArray(meta.branches) ? meta.branches.filter(b => BANK_META[b.bank]) : [];

  renderStats(meta);
  renderBankToggles();
  renderRegionBars();

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
  renderMarkers();
  fitToBranches();
}

document.addEventListener('DOMContentLoaded', bootstrap);
