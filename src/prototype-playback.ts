type Announcement = { key: string };
type Owner = { controller: AbortController; audio?: HTMLAudioElement };

export type PrototypePlaybackOwner = {
  check(): void;
  dispose(): void;
};

type PrototypePlaybackOptions = {
  onError: (message: string | undefined) => void;
};

const latestAudioPath = "/api/prototype/latest";

export function createPrototypePlaybackOwner({
  onError,
}: PrototypePlaybackOptions): PrototypePlaybackOwner {
  let disposed = false;
  let current: Owner | undefined;
  let completedKey: string | undefined;

  const release = (owner: Owner) => {
    if (current !== owner) return;
    current = undefined;
  };

  const fail = (owner: Owner, message: string) => {
    if (disposed || current !== owner) return;
    owner.audio?.pause();
    release(owner);
    onError(message);
  };

  const check = () => {
    if (disposed || current) return;

    const owner: Owner = { controller: new AbortController() };
    current = owner;
    onError(undefined);

    void (async () => {
      try {
        const response = await fetch(latestAudioPath, { signal: owner.controller.signal });
        if (disposed || current !== owner) return;
        if (!response.ok) {
          release(owner);
          return;
        }

        const announcement = (await response.json()) as Announcement;
        if (disposed || current !== owner) return;
        if (!announcement.key) {
          fail(owner, "Could not load the latest audio clip.");
          return;
        }
        if (announcement.key === completedKey) {
          release(owner);
          return;
        }

        const audio = new Audio(`/api/prototype/audio/${announcement.key}`);
        if (disposed || current !== owner) {
          audio.pause();
          return;
        }

        owner.audio = audio;
        audio.onended = () => {
          if (current !== owner) return;
          completedKey = announcement.key;
          release(owner);
        };
        audio.onerror = () => fail(owner, "Audio playback failed. Try again.");
        await audio.play();
        if (disposed || current !== owner) return;
      } catch (error) {
        if (owner.controller.signal.aborted || disposed || current !== owner) return;
        fail(owner, "Could not load the latest audio clip.");
      }
    })();
  };

  return {
    check,
    dispose() {
      if (disposed) return;
      disposed = true;
      const owner = current;
      owner?.controller.abort();
      owner?.audio?.pause();
      current = undefined;
    },
  };
}
