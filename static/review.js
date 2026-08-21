(() => {
  "use strict";

  const fallbackImages = () => {
    document.querySelectorAll("img[data-fallback]").forEach((image) => {
      image.addEventListener("error", () => {
        if (image.src !== image.dataset.fallback) image.src = image.dataset.fallback;
      });
    });
  };

  const openPanel = (name) => {
    const panel = document.querySelector(`[data-action-panel="${name}"]`);
    if (!panel) return;
    panel.open = true;
    panel.scrollIntoView({ behavior: "smooth", block: "center" });
    const focusable = panel.querySelector("select, input:not([type=hidden]), textarea, button[type=submit]");
    if (focusable) focusable.focus();
  };

  const bindShortcuts = () => {
    document.addEventListener("keydown", (event) => {
      if (event.ctrlKey || event.metaKey || event.altKey || event.repeat) return;
      const target = event.target;
      if (target && (target.matches("input, textarea, select") || target.isContentEditable)) return;

      const key = event.key.toLowerCase();
      if (key === "a") openPanel("approve");
      if (key === "n") openPanel("reanalysis");
      if (key === "d") openPanel("reject");
      if (key === "s") {
        const skip = document.getElementById("skip-item");
        if (skip) skip.click();
      }
      if (key === "z") {
        const undo = document.getElementById("undo-last-form");
        if (undo) undo.requestSubmit();
      }
      if (["a", "n", "d", "s", "z"].includes(key)) event.preventDefault();
    });
  };

  const protectSubmissions = () => {
    document.querySelectorAll("form[method=post]").forEach((form) => {
      form.addEventListener("submit", (event) => {
        if (!form.checkValidity()) return;
        const button = event.submitter || form.querySelector("button[type=submit]");
        if (button) {
          button.disabled = true;
          button.dataset.originalText = button.textContent;
          button.textContent = "Salvando…";
        }
      });
    });
  };

  fallbackImages();
  bindShortcuts();
  protectSubmissions();
})();
