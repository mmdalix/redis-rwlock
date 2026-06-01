// Error taxonomy (Spec §14). Callers must be able to tell these apart.

export class RwLockError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message);
    this.name = new.target.name;
    if (options?.cause !== undefined) {
      (this as { cause?: unknown }).cause = options.cause;
    }
  }
}

/** Could not acquire within waitMs. Not an error condition for many callers. */
export class WaitTimeout extends RwLockError {}

/** Redis unreachable / command failed -> fail closed; the acquisition did NOT happen. */
export class BackendUnavailable extends RwLockError {}

/** A held lock's lease expired or was taken (extend returned LOST, or watchdog detected it). */
export class LockLost extends RwLockError {}

/** release/extend on a token not currently held by the caller. */
export class NotHeld extends RwLockError {}

/** The installed module's protocol_version is incompatible (Spec §16). */
export class IncompatibleServerLogic extends RwLockError {}

/** A required capability is unavailable and no acceptable fallback exists. */
export class Unsupported extends RwLockError {}
