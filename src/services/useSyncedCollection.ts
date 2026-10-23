import { useCallback, useRef, useState } from 'react';
import { mergeRemoteList, SaveResult } from '../services/storageService';

interface SyncedCollectionOptions<T> {
  /** Początkowa zawartość (np. z cache localStorage). */
  initial: () => T[];
  /** Zapis lokalnego cache po każdej zmianie listy. */
  saveLocal: (items: T[]) => void;
  /** Zapis na serwerze: tylko zmienione elementy + ID usuniętych. */
  saveRemote: (changed: T[], deletedIds: string[]) => Promise<SaveResult<T>>;
  /** Wywoływane, gdy zapis na serwerze się nie powiódł. */
  onSaveError: (message: string) => void;
  /** Opóźnienie zapisu dla zmian „optymistycznych” (domyślnie 700 ms). */
  debounceMs?: number;
}

/**
 * Kolekcja z kontrolowanym zapisem i bezpieczną synchronizacją zdalną.
 *
 * - commitConfirmed: zapis NAJPIERW na serwerze, stan lokalny zmienia się dopiero po sukcesie
 *   (używane w formularzach „Zapisz” — przy błędzie nic nie jest tracone).
 * - commitOptimistic: natychmiastowa zmiana lokalna + zapis na serwerze (z opóźnieniem lub od razu).
 * - applyRemote: scala dane z serwera/SSE, ale NIGDY nie nadpisuje elementów niezapisanych
 *   ani aktualnie edytowanych (setLockedIds). Zdarzenia z czasu edycji są odroczone i stosowane po zamknięciu formularza.
 */
export function useSyncedCollection<T extends { id: string }>(options: SyncedCollectionOptions<T>) {
  const opts = useRef(options);
  opts.current = options;

  const [items, setItems] = useState<T[]>(() => options.initial());
  const itemsRef = useRef<T[]>(items);
  const dirtyIds = useRef(new Set<string>());
  const pendingDeletedIds = useRef(new Set<string>());
  const lockedIds = useRef(new Set<string>());
  const deferredRemote = useRef<T[] | null>(null);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const queue = useRef<Promise<unknown>>(Promise.resolve());

  const setLocal = useCallback((next: T[]) => {
    itemsRef.current = next;
    setItems(next);
    opts.current.saveLocal(next);
  }, []);

  // Zapisy na serwer wykonują się po kolei — starszy nigdy nie wyprzedzi nowszego.
  const enqueue = useCallback(<R,>(task: () => Promise<R>): Promise<R> => {
    const run = queue.current.then(task, task);
    queue.current = run.catch(() => undefined);
    return run;
  }, []);

  const applyRemote = useCallback(
    (remote: T[]) => {
      if (lockedIds.current.size > 0) {
        deferredRemote.current = remote;
      }
      const visible = remote.filter((r) => !pendingDeletedIds.current.has(r.id));
      const protectedIds = new Set<string>([...dirtyIds.current, ...lockedIds.current]);
      const merged = mergeRemoteList(itemsRef.current, visible, protectedIds);
      if (merged !== itemsRef.current) {
        setLocal(merged);
      }
    },
    [setLocal]
  );

  const setLockedIds = useCallback(
    (ids: string[]) => {
      lockedIds.current = new Set(ids);
      if (ids.length === 0 && deferredRemote.current) {
        const pending = deferredRemote.current;
        deferredRemote.current = null;
        applyRemote(pending);
      }
    },
    [applyRemote]
  );

  const flush = useCallback(async (): Promise<boolean> => {
    if (timer.current) {
      clearTimeout(timer.current);
      timer.current = null;
    }
    const idsSnapshot = Array.from(dirtyIds.current);
    const deletedSnapshot = Array.from(pendingDeletedIds.current);
    if (idsSnapshot.length === 0 && deletedSnapshot.length === 0) return true;

    const sent = itemsRef.current.filter((i) => dirtyIds.current.has(i.id));
    const sentMap = new Map(sent.map((s) => [s.id, s]));

    const result = await enqueue(() => opts.current.saveRemote(sent, deletedSnapshot));
    if (!result.ok) {
      opts.current.onSaveError(result.error || 'Nieznany błąd zapisu');
      return false;
    }

    // Element przestaje być „niezapisany” tylko, jeśli w międzyczasie nie został zmieniony lokalnie.
    idsSnapshot.forEach((id) => {
      const current = itemsRef.current.find((i) => i.id === id);
      if (!current || current === sentMap.get(id)) {
        dirtyIds.current.delete(id);
      }
    });
    deletedSnapshot.forEach((id) => pendingDeletedIds.current.delete(id));

    if (result.items) applyRemote(result.items);
    return true;
  }, [enqueue, applyRemote]);

  const schedule = useCallback(() => {
    if (timer.current) clearTimeout(timer.current);
    timer.current = setTimeout(() => {
      timer.current = null;
      void flush();
    }, opts.current.debounceMs ?? 700);
  }, [flush]);

  const commitOptimistic = useCallback(
    (
      mutate: (current: T[]) => T[],
      changedIds: string[],
      commitOptions?: { deletedIds?: string[]; immediate?: boolean }
    ): Promise<boolean> => {
      setLocal(mutate(itemsRef.current));
      changedIds.forEach((id) => dirtyIds.current.add(id));
      (commitOptions?.deletedIds ?? []).forEach((id) => {
        pendingDeletedIds.current.add(id);
        dirtyIds.current.delete(id);
      });
      if (commitOptions?.immediate) return flush();
      schedule();
      return Promise.resolve(true);
    },
    [setLocal, flush, schedule]
  );

  const commitConfirmed = useCallback(
    async (
      mutate: (current: T[]) => T[],
      changedIds: string[],
      commitOptions?: { deletedIds?: string[] }
    ): Promise<boolean> => {
      const deletedIds = commitOptions?.deletedIds ?? [];
      const before = new Map(changedIds.map((id) => [id, itemsRef.current.find((i) => i.id === id)]));
      const changed = mutate(itemsRef.current).filter((i) => changedIds.includes(i.id));

      const result = await enqueue(() => opts.current.saveRemote(changed, deletedIds));
      if (!result.ok) {
        opts.current.onSaveError(result.error || 'Nieznany błąd zapisu');
        return false;
      }

      const untouchedMeanwhile = changedIds.filter(
        (id) => itemsRef.current.find((i) => i.id === id) === before.get(id)
      );
      setLocal(mutate(itemsRef.current));
      untouchedMeanwhile.forEach((id) => dirtyIds.current.delete(id));
      if (result.items) applyRemote(result.items);
      return true;
    },
    [enqueue, setLocal, applyRemote]
  );

  const replaceAll = useCallback(
    (list: T[]) => {
      dirtyIds.current.clear();
      pendingDeletedIds.current.clear();
      deferredRemote.current = null;
      setLocal(list);
    },
    [setLocal]
  );

  const hasUnsaved = useCallback(
    () => dirtyIds.current.size > 0 || pendingDeletedIds.current.size > 0,
    []
  );

  return {
    items,
    itemsRef,
    applyRemote,
    setLockedIds,
    commitOptimistic,
    commitConfirmed,
    flush,
    replaceAll,
    hasUnsaved,
  };
}
