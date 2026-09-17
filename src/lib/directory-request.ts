const loadError = () => new Error("Unable to load politicians. Please try again.");

/** Retry temporary failures without changing the requested page or filters. */
export async function fetchDirectoryPage(url: string, signal: AbortSignal, fetcher: typeof fetch = fetch) {
  for (let attempt = 0; attempt < 3; attempt++) {
    signal.throwIfAborted();
    let response: Response;
    try {
      response = await fetcher(url, { signal, cache: "no-store" });
    } catch (error) {
      if (signal.aborted) throw error;
      if (attempt === 2) throw loadError();
      await pause(signal, attempt);
      continue;
    }
    if (!response.ok) {
      if (attempt < 2 && [408, 429, 500, 502, 503, 504].includes(response.status)) {
        await pause(signal, attempt);
        continue;
      }
      throw loadError();
    }
    const data = await response.json();
    if (!data || !Array.isArray(data.members) || (data.nextOffset !== null && !Number.isInteger(data.nextOffset))) {
      throw loadError();
    }
    return data;
  }
  throw loadError();
}

function pause(signal: AbortSignal, attempt: number) {
  return new Promise<void>((resolve, reject) => {
    const abort = () => { clearTimeout(timer); reject(signal.reason); };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", abort);
      resolve();
    }, 250 * (attempt + 1));
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) {
      signal.removeEventListener("abort", abort);
      abort();
    }
  });
}
