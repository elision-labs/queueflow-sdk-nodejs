/** Recursive JSON types for ergonomic SDK inputs (payloads, context, metadata). */

export type Json =
  | null
  | boolean
  | number
  | string
  | Json[]
  | { [key: string]: Json };

export type JsonObject = { [key: string]: Json };
