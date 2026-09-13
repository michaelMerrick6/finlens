"use client";
import { useEffect, useRef, useState, type ReactNode } from "react";
import { Icon } from "./icon";
export function Modal({
  title,
  onClose,
  children,
  animated = false,
}: {
  title: string;
  onClose: () => void;
  children: ReactNode;
  animated?: boolean;
}) {
  const [closing, setClosing] = useState(false);
  const closeTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const requestClose = () => {
    if (closeTimer.current) return;
    if (!animated || window.matchMedia('(prefers-reduced-motion: reduce)').matches) { onClose(); return; }
    setClosing(true);
    closeTimer.current = setTimeout(onClose, 140);
  };
  useEffect(() => () => { if (closeTimer.current) clearTimeout(closeTimer.current); }, []);
  const ref = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    const dialog = ref.current;
    const previous = document.activeElement as HTMLElement;
    dialog?.showModal();
    const overflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      dialog?.close();
      document.body.style.overflow = overflow;
      previous?.focus();
    };
  }, []);
  return (
    <dialog
      ref={ref}
      className={`modal${animated ? " modal-animated" : ""}${closing ? " modal-closing" : ""}`}
      onCancel={(e) => { e.preventDefault(); requestClose(); }}
      onClick={(e) => {
        if ((e.target as HTMLElement).closest('[data-modal-close]')) { requestClose(); return; }
        if (e.target === e.currentTarget) {
          const r = e.currentTarget.getBoundingClientRect();
          if (
            e.clientX < r.left ||
            e.clientX > r.right ||
            e.clientY < r.top ||
            e.clientY > r.bottom
          )
            requestClose();
        }
      }}
      aria-label={title}
    >
      <div className="modal-head">
        <span className="eyebrow">{title}</span>
        <button
          className="icon-button"
          onClick={requestClose}
          aria-label="Close dialog"
        >
          <Icon name="close" />
        </button>
      </div>
      {children}
    </dialog>
  );
}
