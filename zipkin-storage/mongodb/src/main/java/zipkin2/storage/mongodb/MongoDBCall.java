/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import java.io.IOException;
import zipkin2.Call;
import zipkin2.Callback;

abstract class MongoDBCall<V> extends Call<V> {

  volatile boolean canceled;
  volatile boolean executed;

  @Override public V execute() throws IOException {
    if (executed) throw new IllegalStateException("already executed");
    executed = true;
    if (canceled) throw new IOException("canceled");
    try {
      return doExecute();
    } catch (RuntimeException e) {
      propagateIfFatal(e);
      throw new IOException(e);
    }
  }

  abstract V doExecute();

  @Override public void enqueue(Callback<V> callback) {
    if (executed) throw new IllegalStateException("already executed");
    executed = true;
    if (canceled) {
      callback.onError(new IOException("canceled"));
      return;
    }
    try {
      callback.onSuccess(doExecute());
    } catch (Throwable t) {
      propagateIfFatal(t);
      callback.onError(t);
    }
  }

  @Override public void cancel() {
    canceled = true;
  }

  @Override public boolean isCanceled() {
    return canceled;
  }
}
