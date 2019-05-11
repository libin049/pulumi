// Copyright 2016-2018, Pulumi Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//
// NOTE: We choose to be purposefully conservative about what details are exposed through these
// interfaces in case we decide to change the implementation drastically later.
//

import { IterablePromiseImpl } from "./enumerablePromise";
import { IterablePromise } from "./interfaces";
import * as sources from "./sources";

export { IterablePromise } from "./interfaces";
export { Queryable, QueryableResource } from "./queryable";

export function from<TSource>(
    source:
        | Iterable<TSource>
        | AsyncIterable<TSource>
        | Promise<Iterable<TSource>>
        | Promise<AsyncIterable<TSource>>,
): IterablePromise<TSource> {
    return IterablePromiseImpl.from(source);
}

export function range(start: number, stop?: number): IterablePromise<number> {
    return IterablePromiseImpl.from(sources.range(start, stop));
}

export function empty<TResult>(): AsyncIterableIterator<TResult> {
    return IterablePromiseImpl.from(sources.from([]));
}

export function singleton<TSource>(t: TSource): AsyncIterableIterator<TSource> {
    return IterablePromiseImpl.from(sources.from([t]));
}

export function repeat<TSource>(t: TSource) {
    IterablePromiseImpl.from(
        (async function*() {
            while (true) {
                yield t;
            }
        })(),
    );
}
