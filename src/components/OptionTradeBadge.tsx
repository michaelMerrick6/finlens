'use client';

import { useId, useState, type FocusEvent, type MouseEvent } from 'react';
import { createPortal } from 'react-dom';

type OptionTradeBadgeProps = {
  label: string;
  tooltip: string;
  className?: string;
};

type TooltipPosition = {
  left: number;
  top: number;
  placement: 'top' | 'bottom';
};

const DEFAULT_BADGE_CLASS_NAME =
  'inline-flex items-center rounded-md border border-orange-500/30 bg-orange-500/10 px-2 py-0.5 text-[11px] font-semibold uppercase tracking-[0.08em] text-orange-300';
const TOOLTIP_WIDTH = 188;
const VIEWPORT_PADDING = 12;
const VERTICAL_GAP = 12;

function splitTooltipLines(tooltip: string): string[] {
  return tooltip
    .split('|')
    .map((part) => part.trim())
    .filter(Boolean);
}

export default function OptionTradeBadge({
  label,
  tooltip,
  className = DEFAULT_BADGE_CLASS_NAME,
}: OptionTradeBadgeProps) {
  const tooltipId = useId();
  const [isOpen, setIsOpen] = useState(false);
  const [position, setPosition] = useState<TooltipPosition | null>(null);
  const tooltipLines = splitTooltipLines(tooltip);

  function updatePosition(target: HTMLElement) {
    const rect = target.getBoundingClientRect();
    const hasRoomAbove = rect.top > 96;
    const nextLeft = Math.min(
      Math.max(rect.left - 6, VIEWPORT_PADDING),
      window.innerWidth - TOOLTIP_WIDTH - VIEWPORT_PADDING,
    );

    setPosition({
      left: nextLeft,
      top: hasRoomAbove ? rect.top - VERTICAL_GAP : rect.bottom + VERTICAL_GAP,
      placement: hasRoomAbove ? 'top' : 'bottom',
    });
  }

  function handleShow(event: MouseEvent<HTMLButtonElement> | FocusEvent<HTMLButtonElement>) {
    updatePosition(event.currentTarget);
    setIsOpen(true);
  }

  function handleMove(event: MouseEvent<HTMLButtonElement>) {
    if (!isOpen) {
      return;
    }
    updatePosition(event.currentTarget);
  }

  function handleHide() {
    setIsOpen(false);
  }

  return (
    <>
      <button
        type="button"
        aria-describedby={isOpen ? tooltipId : undefined}
        onMouseEnter={handleShow}
        onMouseMove={handleMove}
        onMouseLeave={handleHide}
        onClick={handleShow}
        onFocus={handleShow}
        onBlur={handleHide}
        className={className}
        style={{ cursor: 'help' }}
      >
        {label}
      </button>
      {isOpen && position && typeof document !== 'undefined'
        ? createPortal(
            <div
              className="pointer-events-none fixed z-[120]"
              style={{
                left: position.left,
                top: position.top,
                width: `${TOOLTIP_WIDTH}px`,
                transform: position.placement === 'top' ? 'translateY(-100%)' : undefined,
              }}
            >
              <div
                id={tooltipId}
                role="tooltip"
                className="relative w-full rounded-lg px-3 py-2.5 text-left text-xs"
                style={{
                  boxSizing: 'border-box',
                  backgroundColor: '#0b0d12',
                  border: '1px solid rgba(249, 115, 22, 0.24)',
                  boxShadow: '0 18px 40px rgba(0, 0, 0, 0.68)',
                  backdropFilter: 'none',
                  WebkitBackdropFilter: 'none',
                }}
              >
                <div
                  className="absolute left-5 h-2.5 w-2.5 rotate-45"
                  style={{
                    backgroundColor: '#0b0d12',
                    borderLeft: '1px solid rgba(249, 115, 22, 0.24)',
                    borderTop: '1px solid rgba(249, 115, 22, 0.24)',
                    borderRight: position.placement === 'bottom' ? '1px solid rgba(249, 115, 22, 0.24)' : '0',
                    borderBottom: position.placement === 'top' ? '1px solid rgba(249, 115, 22, 0.24)' : '0',
                    top: position.placement === 'bottom' ? '-6px' : undefined,
                    bottom: position.placement === 'top' ? '-6px' : undefined,
                  }}
                />
                <div className="space-y-1">
                  {tooltipLines.map((line, index) => (
                    <div
                      key={line}
                      className={index === 0 ? 'font-semibold text-orange-200' : 'text-zinc-200'}
                    >
                      {line}
                    </div>
                  ))}
                </div>
              </div>
            </div>,
            document.body,
          )
        : null}
    </>
  );
}
