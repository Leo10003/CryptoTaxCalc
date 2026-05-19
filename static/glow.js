(function (global) {
  function pulse(sel) {
    const el = document.querySelector(sel);
    if (!el) return;
    el.style.transition = 'box-shadow .6s ease';
    function tick() {
      el.style.boxShadow = '0 0 24px rgba(12,230,200,.55)';
      setTimeout(() => {
        el.style.boxShadow = '0 0 6px rgba(12,230,200,.15)';
      }, 600);
    }
    tick();
    setInterval(tick, 10000);
  }
  function burst(sel) {
    const el = document.querySelector(sel);
    if (!el) return;
    el.style.transform = 'scale(1.02)';
    setTimeout(() => { el.style.transform = 'scale(1)'; }, 180);
  }
  function sparkle(sel) {
    const el = document.querySelector(sel);
    if (!el) return;
    el.style.transition = 'outline .4s ease';
    el.style.outline = '1px solid rgba(155,124,255,0.6)';
    setTimeout(() => { el.style.outline = '1px solid transparent'; }, 400);
  }

  // Cursor-follow glow used on the landing hero
  function cursorGlow(sel) {
    const glowEl = document.querySelector(sel);
    if (!glowEl) return;
    let raf = 0;
    window.addEventListener(
      "pointermove",
      (e) => {
        if (raf) cancelAnimationFrame(raf);
        raf = requestAnimationFrame(() => {
          glowEl.style.left = e.clientX + "px";
          glowEl.style.top = e.clientY + "px";
        });
      },
      { passive: true }
    );
  }

  global.glow = { pulse, burst, sparkle, cursorGlow };
})(window);

window.updateFXChip = function(summary) {
  const fxChip = document.getElementById("fx-chip");
  const fxRate = document.getElementById("fx-rate");
  const fxBatch = document.getElementById("fx-batch");
  const warnPanel = document.getElementById("warnings-panel");
  if (!fxChip || !fxRate || !fxBatch) return;
  const warnList = document.getElementById("warnings-list");
  if (!summary) return;

  if (summary.fx_context) {
    fxRate.textContent = summary.fx_context.fx_rate_used || "1.0";
    fxBatch.textContent = summary.fx_context.fx_batch_id || "–";
    // Glow if fallback used
    if (parseFloat(summary.fx_context.fx_rate_used || 1.0) === 1.0) {
      fxChip.style.borderColor = "var(--warn)";
      fxChip.style.color = "var(--warn)";
      glow.sparkle("#fx-chip");
    } else {
      fxChip.style.borderColor = "var(--ok)";
      fxChip.style.color = "var(--ok)";
      glow.pulse("#fx-chip");
    }
  }

  if (summary.warnings && summary.warnings.length > 0) {
    warnPanel.style.display = "block";
    warnList.replaceChildren();
    for (const w of summary.warnings) {
      const li = document.createElement("li");
      li.textContent = String(w);
      warnList.appendChild(li);
    }
  } else {
    warnPanel.style.display = "none";
    warnList.replaceChildren();
  }

};

// NEW: reveal-on-scroll utility
  glow.reveal = function(sel=".reveal"){
    const els = document.querySelectorAll(sel);
    if(!els.length) return;
    const io = new IntersectionObserver(entries=>{
      entries.forEach(e=>{
        if(e.isIntersecting){ e.target.classList.add('active'); }
      });
    }, { threshold: .15 });
    els.forEach(el=>io.observe(el));
  };

  window.addEventListener("DOMContentLoaded", () => {
    try { glow.reveal(); } catch (_) {}
    try { glow.cursorGlow("#cursor-glow"); } catch (_) {}
  });
