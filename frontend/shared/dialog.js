/* ==========================================================================
   DARAZ TRADE — CUSTOM THEME DIALOG OVERRIDE
   Automatically replaces native browser alert() popups with branded modals
   ========================================================================== */



   (function () {
  if (!document.querySelector("link[rel*='icon']")) {
    const link = document.createElement('link');
    link.rel = 'icon';
    link.type = 'image/svg+xml';
    link.href = "data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><rect width='100' height='100' rx='22' fill='%23ee4d07'/><path d='M22 72 L44 50 L58 62 L78 34' stroke='%23ffffff' stroke-width='9' stroke-linecap='round' stroke-linejoin='round' fill='none'/><polyline points='64,34 78,34 78,48' stroke='%23ffffff' stroke-width='9' stroke-linecap='round' stroke-linejoin='round' fill='none'/></svg>";
    document.head.appendChild(link);
  }
})();

(function () {
  const queue = [];
  let isShowing = false;

  function detectMeta(text) {
    const lower = String(text).toLowerCase();
    if (lower.includes('success') || lower.includes('approved') || lower.includes('updated') || lower.includes('submitted') || lower.includes('copied') || lower.includes('created') || lower.includes('saved') || lower.includes('sent')) {
      return { icon: '✓', title: 'Daraz Trade Notice' };
    }
    if (lower.includes('install')) {
      return { icon: '📲', title: 'Install Daraz Trade' };
    }
    return { icon: '!', title: 'Daraz Trade Alert' };
  }

  function renderNext() {
    if (isShowing || queue.length === 0) return;
    isShowing = true;

    const message = queue.shift();
    const { icon, title } = detectMeta(message);

    const backdrop = document.createElement('div');
    backdrop.className = 'dt-alert-backdrop';
    backdrop.innerHTML = `
      <div class="dt-alert-box" role="alertdialog" aria-modal="true">
        <div class="dt-alert-icon">${icon}</div>
        <div class="dt-alert-title">${title}</div>
        <div class="dt-alert-msg"></div>
        <button type="button" class="dt-alert-btn">OK</button>
      </div>
    `;

    backdrop.querySelector('.dt-alert-msg').textContent = String(message);

    const close = () => {
      backdrop.remove();
      isShowing = false;
      renderNext();
    };

    backdrop.querySelector('.dt-alert-btn').addEventListener('click', close);
    backdrop.addEventListener('click', (e) => {
      if (e.target === backdrop) close();
    });

    document.body.appendChild(backdrop);
    backdrop.querySelector('.dt-alert-btn').focus();
  }

  window.alert = function (message) {
    queue.push(message);
    if (document.body) {
      renderNext();
    } else {
      window.addEventListener('DOMContentLoaded', renderNext, { once: true });
    }
  };

  
})();

/* ==========================================================================
   DARAZ TRADE — LIVE PAKISTANI SOCIAL-PROOF ACTIVITY TOAST
   Runs automatically on Login/Register & Trader Dashboard screens
   ========================================================================== */
(function () {
  // Do not show on Admin Portal pages
  if (window.location.pathname.includes('/admin')) return;

  const fallbackNames = [
    'Shamir K.', 'Javeria N.', 'Zainab S.', 'Bilal A.', 'Hamza R.',
    'Ayesha T.', 'Usman M.', 'Fatima Z.', 'Omer F.', 'Hira B.',
    'Tariq M.', 'Sana K.', 'Daniyal H.', 'Mahnoor R.', 'Kamran J.'
  ];

  function createToastElement() {
    if (document.getElementById('dtLiveToast')) return document.getElementById('dtLiveToast');

    const el = document.createElement('div');
    el.id = 'dtLiveToast';
    Object.assign(el.style, {
      position: 'fixed',
      bottom: '20px',
      right: '20px',
      background: '#ffffff',
      borderLeft: '5px solid #ee4d07',
      boxShadow: '0 12px 30px rgba(15, 23, 42, 0.22)',
      borderRadius: '12px',
      padding: '12px 16px',
      display: 'flex',
      alignItems: 'center',
      gap: '12px',
      zIndex: '9998',
      opacity: '0',
      transform: 'translateY(16px)',
      transition: 'all 0.35s ease',
      pointerEvents: 'none',
      maxWidth: '320px',
      fontFamily: "'Plus Jakarta Sans', sans-serif"
    });

    el.innerHTML = `
      <div id="dtToastIcon" style="font-size:1.35rem;">📥</div>
      <div>
        <div id="dtToastText" style="font-size:0.86rem; font-weight:700; color:#0f172a;">Shamir K. deposited PKR 10,000</div>
        <div id="dtToastTime" style="font-size:0.75rem; color:#ee4d07; font-weight:600;">● Just now</div>
      </div>
    `;
    document.body.appendChild(el);
    return el;
  }

  async function triggerToast() {
    if (!document.body) return;
    const toast = createToastElement();

    let item;
    try {
      const res = await fetch('/api/public/live-activity');
      if (!res.ok) throw new Error('fallback');
      item = await res.json();
    } catch (e) {
      const name = fallbackNames[Math.floor(Math.random() * fallbackNames.length)];
      const actions = ['deposited', 'withdrew', 'won'];
      const action = actions[Math.floor(Math.random() * actions.length)];
      const amt = ((Math.floor(Math.random() * 75) + 5) * 1000).toLocaleString();
      item = { text: `${name} ${action} PKR ${amt}`, action, timeAgo: `${Math.floor(Math.random() * 9) + 1} mins ago` };
    }

    document.getElementById('dtToastIcon').textContent =
      item.action === 'deposited' ? '📥' : item.action === 'withdrew' ? '💸' : '🏆';
    document.getElementById('dtToastText').textContent = item.text;
    document.getElementById('dtToastTime').textContent = `● ${item.timeAgo}`;

    toast.style.opacity = '1';
    toast.style.transform = 'translateY(0)';

    setTimeout(() => {
      toast.style.opacity = '0';
      toast.style.transform = 'translateY(16px)';
    }, 4500);
  }

  window.addEventListener('DOMContentLoaded', () => {
    setTimeout(triggerToast, 1500);
    setInterval(triggerToast, 8000);
  });
})();