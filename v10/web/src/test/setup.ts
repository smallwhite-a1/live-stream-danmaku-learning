import "@testing-library/jest-dom/vitest";

// React falls back to prefixed animation events when jsdom lacks AnimationEvent.
if (!("AnimationEvent" in window)) {
  Object.defineProperty(window, "AnimationEvent", {
    configurable: true,
    value: Event,
  });
}
