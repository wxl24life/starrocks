// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.starrocks.catalog.Function.CompareMode.IS_IDENTICAL;
import static com.starrocks.catalog.Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF;
import static com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter.DEFAULT_TYPE_CAST_RULE;

public class ScalarOperatorUtil {

    public static Stream<ScalarOperator> getStream(ScalarOperator operator) {
        return Stream.concat(Stream.of(operator),
                operator.getChildren().stream().flatMap(ScalarOperatorUtil::getStream));
    }

    public static CallOperator buildMultiCountDistinct(CallOperator oldFunctionCall) {
        Function searchDesc = new Function(new FunctionName(FunctionSet.MULTI_DISTINCT_COUNT),
                oldFunctionCall.getFunction().getArgs(), Type.INVALID, false);
        Function fn = GlobalStateMgr.getCurrentState().getFunction(searchDesc, IS_NONSTRICT_SUPERTYPE_OF);
        if (fn == null) {
            return null;
        }

        ScalarOperatorRewriter scalarOpRewriter = new ScalarOperatorRewriter();
        return (CallOperator) scalarOpRewriter.rewrite(
                new CallOperator(FunctionSet.MULTI_DISTINCT_COUNT, fn.getReturnType(), oldFunctionCall.getChildren(),
                        fn),
                DEFAULT_TYPE_CAST_RULE);
    }

    public static CallOperator buildSum(ColumnRefOperator arg) {
        Preconditions.checkArgument(arg.getType() == Type.BIGINT);
        Function searchDesc = new Function(new FunctionName(FunctionSet.SUM),
                new Type[] {arg.getType()}, arg.getType(), false);
        Function fn = GlobalStateMgr.getCurrentState().getFunction(searchDesc, IS_NONSTRICT_SUPERTYPE_OF);
        ScalarOperatorRewriter scalarOpRewriter = new ScalarOperatorRewriter();
        return (CallOperator) scalarOpRewriter.rewrite(
                new CallOperator(FunctionSet.SUM, fn.getReturnType(), Lists.newArrayList(arg), fn),
                DEFAULT_TYPE_CAST_RULE);
    }

    public static Function findArithmeticFunction(CallOperator call, String fnName) {
        return findArithmeticFunction(call.getFunction().getArgs(), fnName);
    }

    public static Function findArithmeticFunction(Type[] argsType, String fnName) {
        return Expr.getBuiltinFunction(fnName, argsType, IS_IDENTICAL);
    }

    public static Function findSumFn(Type[] argTypes) {
        Function sumFn = findArithmeticFunction(argTypes, FunctionSet.SUM);
        Preconditions.checkState(sumFn != null);
        Function newFn = sumFn.copy();
        if (argTypes[0].isDecimalV3()) {
            newFn.setArgsType(argTypes);
            newFn.setRetType(ScalarType.createDecimalV3NarrowestType(38,
                    ((ScalarType) argTypes[0]).getScalarScale()));
        }
        return newFn;
    }

    public static boolean isSimpleLike(ScalarOperator op) {
        return Utils.downcast(op, LikePredicateOperator.class)
                .map(likeOp -> !likeOp.isRegexp() &&
                        likeOp.getChild(0).isColumnRef() &&
                        likeOp.getChild(1).isConstantRef())
                .orElse(false);
    }

    public static boolean isLiteral(ScalarOperator scalarOperator) {
        return scalarOperator.accept(new ScalarOperatorVisitor<Boolean, Void>() {
            @Override
            public Boolean visitConstant(ConstantOperator literal, Void context) {
                return true;
            }

            @Override
            public Boolean visitCastOperator(CastOperator operator, Void context) {
                return operator.getChild(0).accept(this, null);
            }

            @Override
            public Boolean visitArray(ArrayOperator array, Void context) {
                for (ScalarOperator child : array.getChildren()) {
                    if (!child.accept(this, null)) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public Boolean visit(ScalarOperator scalarOperator, Void context) {
                return null;
            }
        }, null);
    }

    public static boolean isFloatArray(ScalarOperator scalarOperator) {
        Type type = scalarOperator.getType();
        if (type instanceof ArrayType) {
            Type itemType = ((ArrayType) type).getItemType();
            return itemType.isFloat();
        }
        return false;
    }

    public static boolean isSimpleNotLike(ScalarOperator op) {
        return Utils.downcast(op, CompoundPredicateOperator.class)
                .map(compOp -> compOp.isNot() && isSimpleLike(compOp.getChild(0)))
                .orElse(false);
    }

    public static ScalarOperator and(ScalarOperator lhs, ScalarOperator rhs) {
        return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, lhs, rhs);
    }

    public static ScalarOperator and(List<ScalarOperator> xs) {
        if (xs.size() > 2) {
            return and(xs.get(0), and(xs.subList(1, xs.size())));
        } else if (xs.size() == 2) {
            return and(xs.get(0), xs.get(1));
        } else {
            throw new RuntimeException("and requires at least 2 arguments");
        }
    }

    public static ScalarOperator rewrite(ScalarOperator operator, Map<String, ColumnRefOperator> columnRefMap) {
        return operator.accept(new ScalarOperatorVisitor<ScalarOperator, Void>() {
            @Override
            public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
                ScalarOperator clone = scalarOperator.clone();
                for (int i = 0; i < clone.getChildren().size(); i++) {
                    clone.setChild(i, clone.getChild(i).accept(this, null));
                }
                return clone;
            }
            @Override
            public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
                return columnRefMap.get(variable.getName());
            }
        }, null);
    }

    public static ScalarOperator rewrite(ScalarOperator operator, ScalarOperator part, ScalarOperator replace) {
        return operator.accept(new ScalarOperatorVisitor<ScalarOperator, Void>() {
            @Override
            public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
                if (scalarOperator.equals(part)) {
                    return replace;
                }
                ScalarOperator clone = scalarOperator.clone();
                for (int i = 0; i < clone.getChildren().size(); i++) {
                    clone.setChild(i, clone.getChild(i).accept(this, null));
                }
                return clone;
            }
        }, null);
    }

    public static boolean isEquivalentIgnoreCast(ScalarOperator lhs, ScalarOperator rhs) {
        if (lhs instanceof CastOperator) {
            return isEquivalentIgnoreCast(lhs.getChild(0), rhs);
        }
        if (rhs instanceof CastOperator) {
            return isEquivalentIgnoreCast(lhs, rhs.getChild(0));
        }
        return lhs.equals(rhs);
    }
}
