package cc.whohow.elasticsearch.impl;

import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class CompletableResponseListener implements ResponseListener, Supplier<CompletableFuture<Response>> {
    private CompletableFuture<Response> response = new CompletableFuture<>();

    @Override
    public void onSuccess(Response response) {
        this.response.complete(response);
    }

    @Override
    public void onFailure(Exception e) {
        this.response.completeExceptionally(e);
    }

    @Override
    public CompletableFuture<Response> get() {
        return response;
    }
}
