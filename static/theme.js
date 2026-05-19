// static/theme.js
// Disable animated background + video on demo dashboard
if (location.pathname.includes("/demo/dashboard")) {
  window.addEventListener("DOMContentLoaded", () => {
    if (!document.body) return;
    document.body.classList.add("no-animated-bg");
    document.body.classList.remove("has-bg-video");
  });
}

// Use calm gradient (no video) on workspace + history
if (location.pathname.startsWith("/workspace") || location.pathname.startsWith("/history")) {
  window.addEventListener("DOMContentLoaded", () => {
    if (!document.body) return;
    document.body.classList.add("no-video-bg");
    document.body.classList.remove("has-bg-video");
  });
}

(function() {
  const html = document.documentElement;

  // 1) Load saved theme (or default to dark)
  const saved = localStorage.getItem('ctc_theme');
  if (saved === 'light') {
    html.setAttribute('data-theme', 'light');
  } else {
    html.setAttribute('data-theme', 'dark');
  }

  // 2) Keep logos in sync with theme
  function applyLogo() {
    const isLight = html.getAttribute('data-theme') === 'light';
    const brand  = document.getElementById('brandMark');
    const footer = document.getElementById('footerMark');
    const status = document.getElementById('statusMark');

    const logoVariant = isLight ? 'light' : 'dark';
    if (brand)  brand.src  = '/logo/icon?theme=' + logoVariant;
    if (footer) footer.src = '/logo/icon?theme=' + logoVariant;
    if (status) status.src = '/logo/icon?theme=' + logoVariant;
  }

  // Run once immediately (in case header is already parsed)
  applyLogo();

  // Wire up toggle once DOM is ready
  window.addEventListener('DOMContentLoaded', () => {
    applyLogo();

    const tgl = document.getElementById('themeToggle');
    if (!tgl) return;

    // Helper: briefly enable CSS transitions when theme changes
    function startThemeTransition() {
      html.classList.add('theme-transition');
      window.setTimeout(() => {
        html.classList.remove('theme-transition');
      }, 360);
    }

    // Set initial aria state
    const isLight = html.getAttribute('data-theme') === 'light';
    tgl.setAttribute('aria-pressed', String(isLight));

    tgl.addEventListener('click', () => {
      startThemeTransition(); // <-- start smooth fade

      const nowLight = html.getAttribute('data-theme') !== 'light';
      html.setAttribute('data-theme', nowLight ? 'light' : 'dark');
      localStorage.setItem('ctc_theme', nowLight ? 'light' : 'dark');
      tgl.setAttribute('aria-pressed', String(nowLight));
      applyLogo();

      // premium micro-animation: brief glow ring + slight lift
      tgl.classList.remove('theme-toggle-anim');
      void tgl.offsetWidth;                // force reflow to restart animation
      tgl.classList.add('theme-toggle-anim');
      setTimeout(() => tgl.classList.remove('theme-toggle-anim'), 220);
    });
  });
})();
