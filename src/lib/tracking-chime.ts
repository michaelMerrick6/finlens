// Initialize during the click so browsers can allow audio after the save finishes.
export function prepareTrackingChime() {
  let context: AudioContext | undefined;
  try {
    context = new AudioContext();
    void context.resume().catch(() => {});
  } catch {
    // Audio is optional; tracking must still work if it is unavailable.
  }
  const close = () => {
    if (context && context.state !== 'closed') void context.close().catch(() => {});
  };
  return {
    cancel: close,
    play() {
      if (!context || context.state !== 'running') { close(); return; }
      try {
        const start = context.currentTime;
        [660, 880].forEach((frequency, index) => {
          const tone = context!.createOscillator();
          const gain = context!.createGain();
          const at = start + index * 0.065;
          tone.type = 'sine';
          tone.frequency.value = frequency;
          gain.gain.setValueAtTime(0, at);
          gain.gain.linearRampToValueAtTime(0.025, at + 0.012);
          gain.gain.exponentialRampToValueAtTime(0.0001, at + 0.17);
          tone.connect(gain);
          gain.connect(context!.destination);
          if (index === 1) tone.onended = close;
          tone.start(at);
          tone.stop(at + 0.18);
        });
      } catch { close(); }
    },
  };
}

// A soft, warm tap, generated locally with no audio download.
export function playTrackingOpenSound() {
  try {
    const context = new AudioContext();
    const close = () => { void context.close().catch(() => {}); };
    void context.resume().then(() => {
      try {
        const tone = context.createOscillator();
        const gain = context.createGain();
        const at = context.currentTime;
        tone.type = 'sine';
        tone.frequency.setValueAtTime(390, at);
        tone.frequency.exponentialRampToValueAtTime(310, at + 0.07);
        gain.gain.setValueAtTime(0, at);
        gain.gain.linearRampToValueAtTime(0.018, at + 0.006);
        gain.gain.exponentialRampToValueAtTime(0.0001, at + 0.095);
        tone.connect(gain);
        gain.connect(context.destination);
        tone.onended = close;
        tone.start(at);
        tone.stop(at + 0.11);
      } catch { close(); }
    }).catch(close);
  } catch { /* Optional sound must never block the popup. */ }
}
