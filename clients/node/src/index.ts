export { RwLock, keyPrefix, type LockScope } from "./rwlock.js";
export { LockHandle, type HandleOwner, type LockHandleData } from "./handle.js";
export {
  RwLockError,
  WaitTimeoutError,
  BackendUnavailableError,
  LockLostError,
  NotHeldError,
  IncompatibleServerLogicError,
  UnsupportedError,
} from "./errors.js";
export {
  DEFAULTS,
  type AcquireOptions,
  type Fairness,
  type LockMode,
  type RwLockConfig,
} from "./types.js";
