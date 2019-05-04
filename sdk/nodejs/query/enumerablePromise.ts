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

import { IterableBase } from "./base";
import { Grouping, IterablePromise, Operator, QuerySource, OrderKey } from "./interfaces";
import {
    aggregate,
    all,
    any,
    average,
    concat,
    contains,
    count,
    defaultIfEmpty,
    distinct,
    elementAt,
    elementAtOrDefault,
    except,
    filter,
    first,
    firstOrDefault,
    flatMap,
    groupBy,
    groupJoin,
    intersect,
    join,
    last,
    lastOrDefault,
    map,
    max,
    min,
    ofType,
    orderBy,
    orderByDescending,
    reverse,
    single,
    singleOrDefault,
    skip,
    skipWhile,
    sum,
    take,
    takeWhile,
    toArray,
    toMap,
    union,
    zip,
} from "./operators";
import { from, range } from "./sources";

export class IterablePromiseImpl<TSource> extends IterableBase<TSource>
    implements IterablePromise<TSource> {
    //
    // Constructors.
    //

    public static from<TSource>(source: QuerySource<TSource>): IterablePromiseImpl<TSource> {
        return new IterablePromiseImpl(from(source));
    }

    protected constructor(source: AsyncIterableIterator<TSource>) {
        super(source);
    }

    //
    // Restriction operators.
    //

    public filter(f: (t: TSource, i: number) => boolean): IterablePromise<TSource> {
        return this.pipe(filter(f));
    }

    //
    // Projection operators.
    //

    public flatMap<TInner, TResult = TInner>(
        selector: (t: TSource, index: number) => QuerySource<TInner>,
        resultSelector: (t: TSource, ti: TInner) => TResult | Promise<TResult> = (t, ti) =>
            <TResult>(<unknown>ti),
    ): IterablePromise<TResult> {
        return this.pipe(flatMap(selector, resultSelector));
    }

    public map<TResult>(
        f: (t: TSource, i: number) => TResult | Promise<TResult>,
    ): IterablePromise<TResult> {
        return this.pipe(map(f));
    }

    //
    // Partitioning operators.
    //

    public skip(n: number): IterablePromise<TSource> {
        return this.pipe(skip(n));
    }

    public skipWhile(
        predicate: (t: TSource, i: number) => boolean | Promise<boolean>,
    ): IterablePromise<TSource> {
        return this.pipe(skipWhile(predicate));
    }

    public take(n: number): IterablePromise<TSource> {
        return this.pipe(take(n));
    }

    public takeWhile(
        predicate: (t: TSource, i: number) => boolean | Promise<boolean>,
    ): IterablePromise<TSource> {
        return this.pipe(takeWhile(predicate));
    }

    //
    // Join operators.
    //

    public join<TInner, TKey, TResult>(
        inner: QuerySource<TInner>,
        outerKeySelector: (to: TSource) => TKey | Promise<TKey>,
        innerKeySelector: (ti: TInner) => TKey | Promise<TKey>,
        resultSelector: (to: TSource, ti: TInner) => TResult | Promise<TResult>,
    ): IterablePromise<TResult> {
        return this.pipe(join(from(inner), outerKeySelector, innerKeySelector, resultSelector));
    }

    public groupJoin<TInner, TKey, TResult>(
        inner: QuerySource<TInner>,
        outerKeySelector: (to: TSource) => TKey | Promise<TKey>,
        innerKeySelector: (ti: TInner) => TKey | Promise<TKey>,
        resultSelector: (to: TSource, ti: QuerySource<TInner>) => TResult | Promise<TResult>,
    ): IterablePromise<TResult> {
        return this.pipe(
            groupJoin(from(inner), outerKeySelector, innerKeySelector, resultSelector),
        );
    }

    //
    // Concatenation operators.
    //

    public concat(iter: QuerySource<TSource>): IterablePromise<TSource> {
        return this.pipe(concat(from(iter)));
    }

    //
    // Ordering operators.
    //

    public reverse(): IterablePromise<TSource> {
        return this.pipe(reverse());
    }

    public orderBy(
        keySelector: (t: TSource) => OrderKey | Promise<OrderKey>,
    ): IterablePromise<TSource> {
        return this.pipe(orderBy(keySelector));
    }

    public orderByDescending(
        keySelector: (t: TSource) => OrderKey | Promise<OrderKey>,
    ): IterablePromise<TSource> {
        return this.pipe(orderByDescending(keySelector));
    }

    //
    // Grouping operators.
    //

    public groupBy<TKey, TResult = TSource>(
        keySelector: (t: TSource) => TKey | Promise<TKey>,
        elementSelector?: (t: TSource) => TResult | Promise<TResult>,
    ): IterablePromise<Grouping<TKey, TResult>> {
        return this.pipe(async function*(source: AsyncIterableIterator<TSource>) {
            const groups = await groupBy(keySelector, elementSelector)(source);
            for await (const group of groups) {
                yield new GroupingImpl(group.key, from(group));
            }
        });
    }

    //
    // Set operators.
    //

    public distinct(): IterablePromise<TSource> {
        return this.pipe(distinct());
    }

    public union(second: QuerySource<TSource>): IterablePromise<TSource> {
        return this.pipe(union(from(second)));
    }

    public intersect(second: QuerySource<TSource>): IterablePromise<TSource> {
        return this.pipe(intersect(from(second)));
    }

    public except(second: QuerySource<TSource>): IterablePromise<TSource> {
        return this.pipe(except(from(second)));
    }

    //
    // Element operators.
    //

    public first(predicate?: (t: TSource) => boolean | Promise<boolean>): Promise<TSource> {
        return first(predicate)(this);
    }

    public firstOrDefault(
        defaultValue: TSource,
        predicate?: (t: TSource) => boolean | Promise<boolean>,
    ): Promise<TSource> {
        return firstOrDefault(defaultValue, predicate)(this);
    }

    public last(predicate?: (t: TSource) => boolean | Promise<boolean>): Promise<TSource> {
        return last(predicate)(this);
    }

    public lastOrDefault(
        defaultValue: TSource,
        predicate?: (t: TSource) => boolean | Promise<boolean>,
    ): Promise<TSource> {
        return lastOrDefault(defaultValue, predicate)(this);
    }

    public single(predicate?: (t: TSource) => boolean | Promise<boolean>): Promise<TSource> {
        return single(predicate)(this);
    }

    public singleOrDefault(
        defaultValue: TSource,
        predicate?: (t: TSource) => boolean | Promise<boolean>,
    ): Promise<TSource> {
        return singleOrDefault(defaultValue, predicate)(this);
    }

    public elementAt(index: number): Promise<TSource> {
        return elementAt<TSource>(index)(this);
    }

    public elementAtOrDefault(defaultValue: TSource, index: number): Promise<TSource> {
        return elementAtOrDefault(defaultValue, index)(this);
    }

    public defaultIfEmpty(defaultValue: TSource): IterablePromise<TSource> {
        return this.pipe(defaultIfEmpty(defaultValue));
    }

    //
    // Quantifiers.
    //

    public any(predicate?: (t: TSource) => boolean | Promise<boolean>): Promise<boolean> {
        return any(predicate)(this);
    }

    public all(predicate: (t: TSource) => boolean | Promise<boolean>): Promise<boolean> {
        return all(predicate)(this);
    }

    public contains(value: TSource): Promise<boolean> {
        return contains(value)(this);
    }

    //
    // Aggregate operators.
    //

    public count(predicate?: (t: TSource) => boolean | Promise<boolean>): Promise<number> {
        return count(predicate)(this);
    }

    public sum(): TSource extends number ? Promise<number> : never;
    public sum(selector?: (t: TSource) => number | Promise<number>): Promise<number>;
    public sum(selector?: (t: any) => number | Promise<number>): Promise<number> {
        return sum(selector)(this);
    }

    public min(): TSource extends number ? Promise<number> : never;
    public min(selector?: (t: TSource) => number | Promise<number>): Promise<number>;
    public min(selector?: (t: any) => number | Promise<number>): any {
        return min(selector)(this);
    }

    public max(): TSource extends number ? Promise<number> : never;
    public max(selector?: (t: TSource) => number | Promise<number>): Promise<number>;
    public max(selector?: (t: any) => number | Promise<number>): any {
        return max(selector)(this);
    }

    public average(): TSource extends number ? Promise<number> : never;
    public average(selector?: (t: TSource) => number | Promise<number>): Promise<number>;
    public average(selector?: (t: any) => number | Promise<number>): any {
        return average(selector)(this);
    }

    public aggregate<TAccumulate>(
        seed: TAccumulate,
        func: (acc: TAccumulate, t: TSource) => TAccumulate | Promise<TAccumulate>,
    ): Promise<TAccumulate> {
        return aggregate(seed, func)(this);
    }

    //
    // Eval operators.
    //

    public async toArray(): Promise<TSource[]> {
        return toArray<TSource>()(this);
    }

    public toMap<TKey, TResult = TSource>(
        keySelector: (t: TSource) => TKey | Promise<TKey>,
        elementSelector: (t: TSource) => TResult | Promise<TResult>,
    ): Promise<Map<TKey, TResult>> {
        return toMap(keySelector, elementSelector)(this);
    }

    public ofType<TResult>(typeGuard: (o: any) => o is TResult): IterablePromise<TResult> {
        return this.pipe(ofType(typeGuard));
    }

    public async forEach(f: (t: TSource) => void | Promise<void>): Promise<void> {
        for await (const t of this) {
            f(t);
        }
    }

    //
    // Iterable interop operators.
    //

    pipe(): IterablePromise<TSource>;
    pipe<TResult>(op: Operator<TSource, TResult>): IterablePromise<TResult>;
    pipe<TResult1, TResult2>(
        op1: Operator<TSource, TResult1>,
        op2: Operator<TResult1, TResult2>,
    ): IterablePromise<TResult2>;
    pipe<TResult1, TResult2, TResult3>(
        op1: Operator<TSource, TResult1>,
        op2: Operator<TResult1, TResult2>,
        op3: Operator<TResult2, TResult3>,
    ): IterablePromise<TResult3>;
    pipe<TResult1, TResult2, TResult3, TResult4>(
        op1: Operator<TSource, TResult1>,
        op2: Operator<TResult1, TResult2>,
        op3: Operator<TResult2, TResult3>,
        op4: Operator<TResult3, TResult4>,
    ): IterablePromise<TResult4>;
    pipe<TResult1, TResult2, TResult3, TResult4, TResult5>(
        op1: Operator<TSource, TResult1>,
        op2: Operator<TResult1, TResult2>,
        op3: Operator<TResult2, TResult3>,
        op4: Operator<TResult3, TResult4>,
        op5: Operator<TResult4, TResult5>,
    ): IterablePromise<TResult5>;
    pipe<TResult1, TResult2, TResult3, TResult4, TResult5, TResult6>(
        op1: Operator<TSource, TResult1>,
        op2: Operator<TResult1, TResult2>,
        op3: Operator<TResult2, TResult3>,
        op4: Operator<TResult3, TResult4>,
        op5: Operator<TResult4, TResult5>,
        op6: Operator<TResult5, TResult6>,
    ): IterablePromise<TResult6>;
    pipe<TResult1, TResult2, TResult3, TResult4, TResult5, TResult6, TResult7>(
        op1: Operator<TSource, TResult1>,
        op2: Operator<TResult1, TResult2>,
        op3: Operator<TResult2, TResult3>,
        op4: Operator<TResult3, TResult4>,
        op5: Operator<TResult4, TResult5>,
        op6: Operator<TResult5, TResult6>,
        op7: Operator<TResult6, TResult7>,
    ): IterablePromise<TResult7>;
    pipe<TResult1, TResult2, TResult3, TResult4, TResult5, TResult6, TResult7, TResult8>(
        op1: Operator<TSource, TResult1>,
        op2: Operator<TResult1, TResult2>,
        op3: Operator<TResult2, TResult3>,
        op4: Operator<TResult3, TResult4>,
        op5: Operator<TResult4, TResult5>,
        op6: Operator<TResult5, TResult6>,
        op7: Operator<TResult6, TResult7>,
        op8: Operator<TResult7, TResult8>,
    ): IterablePromise<TResult8>;
    pipe<TResult1, TResult2, TResult3, TResult4, TResult5, TResult6, TResult7, TResult8, TResult9>(
        op1: Operator<TSource, TResult1>,
        op2: Operator<TResult1, TResult2>,
        op3: Operator<TResult2, TResult3>,
        op4: Operator<TResult3, TResult4>,
        op5: Operator<TResult4, TResult5>,
        op6: Operator<TResult5, TResult6>,
        op7: Operator<TResult6, TResult7>,
        op8: Operator<TResult7, TResult8>,
        op9: Operator<TResult8, TResult9>,
        ...ops: Operator<any, any>[]
    ): IterablePromise<TResult9>;
    public pipe(...ops: Operator<any, any>[]): IterablePromise<any> {
        return new IterablePromiseImpl(
            (async function*(source: AsyncIterableIterator<TSource>) {
                let newSource = source;
                for (const op of ops) {
                    newSource = op(newSource);
                }

                for await (const t of newSource) {
                    yield t;
                }
            })(this),
        );
    }
}

export class GroupingImpl<TKey, TSource> extends IterablePromiseImpl<TSource>
    implements Grouping<TKey, TSource> {
    constructor(public readonly key: TKey, group: AsyncIterableIterator<TSource>) {
        super(group);
    }
}
