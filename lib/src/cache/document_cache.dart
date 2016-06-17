library document_db.document_cache;

import 'dart:async';

enum SetAction { SET, ADD, REPLACE }

abstract class DocumentCache {
  Future clear();

  Future<String> get(String key);
  Future<Map<String, String>> getAll(Iterable<String> keys);

  Future remove(String key);
  Future removeAll(Iterable<String> keys);

  Future set(String key, String value, {SetAction action: SetAction.SET});
  Future setAll(Map<String, String> keysAndValues,
      {SetAction action: SetAction.SET});
}
