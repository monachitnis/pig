package org.apache.pig.newplan.logical.rules;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.newplan.optimizer.Transformer;

public class NestedForEachUserFunc extends Rule {

    public NestedForEachUserFunc(String n) {
        super(n, false);
    }

    @Override
    protected OperatorPlan buildPattern() {
        LogicalPlan plan = new LogicalPlan();
        LogicalRelationalOperator foreach = new LOForEach(plan);

        plan.add(foreach);
        return plan;
    }

    @Override
    public Transformer getNewTransformer() {
        return new NestedForEachUserFuncTransformer();
    }

    public class NestedForEachUserFuncTransformer extends Transformer {

        LOForEach foreach = null;
        Set<Pair<Pair<Long, Integer>, UserFuncExpression>> userFuncSet;
        Iterator<Pair<Pair<Long, Integer>, UserFuncExpression>> listIter;
        OperatorSubPlan subPlan = null;

        @Override
        public boolean check(OperatorPlan matched) throws FrontendException {
            boolean nested = false;
            Iterator<Operator> iter = matched.getOperators();
            while (iter.hasNext()) {
                Operator op = iter.next();
                if (op instanceof LOForEach) {
                    foreach = (LOForEach) op;
                    break;
                }
            }
            // 1. infer nested foreach
            OperatorPlan innerPlan = foreach.getPlan();
            Operator pred = innerPlan.getPredecessors(foreach).get(0);

            if (pred instanceof LOForEach) {
                nested = true;
            }
            // 2. traverse and record userfunc instances and count
            if (nested) {
                OperatorPlan inner = foreach.getInnerPlan();
                Iterator<Operator> iter2 = inner.getOperators();
                while (iter2.hasNext()) {
                    Operator op = iter2.next();
                    if (op instanceof LOGenerate) {
                        List<LogicalExpressionPlan> generatePlan = ((LOGenerate) op).getOutputPlans();
                        Iterator<LogicalExpressionPlan> lepIter = generatePlan.iterator();
                        while (lepIter.hasNext()) {
                            Iterator<Operator> expIter = lepIter.next().getOperators();
                            while (expIter.hasNext()) {
                                Operator expOp = expIter.next();
                                if (expOp instanceof UserFuncExpression) {
                                    Operator column = expOp.getPlan().getSuccessors(expOp).get(0);
                                    long col = ((ProjectExpression)column).getColNum();
                                    if (userFuncSet == null) {
                                        userFuncSet = new HashSet<Pair<Pair<Long, Integer>, UserFuncExpression>>();
                                    }
                                    listIter = userFuncSet.iterator();
                                    while (listIter.hasNext()) {
                                        Pair<Pair<Long, Integer>, UserFuncExpression> func = listIter.next();
                                        if (func.second == expOp) {
                                            func.first.second++;
                                        }
                                    }
                                    userFuncSet.add(new Pair(new Pair(col, 0), expOp));
                                }
                                break;
                            }
                        }
                        break;
                    }
                }
                if (userFuncSet != null && !userFuncSet.isEmpty()) {
                    listIter = userFuncSet.iterator();
                    while (listIter.hasNext()) {
                        Pair<Pair<Long, Integer>, UserFuncExpression> func = listIter.next();
                        if (func.first.second > 1) {
                            // even if true for one, match is overall true
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        @Override
        public void transform(OperatorPlan matched) throws FrontendException {
            LogicalPlan innerPlan = foreach.getInnerPlan();
            List<Operator> sinks1 = innerPlan.getSinks();
            subPlan = new OperatorSubPlan(currentPlan);
            LogicalExpression userfunc = null;

            LogicalRelationalOperator foreach1 = new LOForEach(innerPlan);
            subPlan.add(foreach1);
            currentPlan.connect(foreach, foreach1);
            currentPlan.connect(foreach1, userfunc);

            LogicalRelationalOperator foreach2 = new LOForEach(innerPlan);
            subPlan.add(foreach2);
            // get the first sink from foreach --> BinCond
            Operator expOp = sinks1.get(0);
            currentPlan.connect(foreach, foreach2);
            currentPlan.connect(foreach2, expOp);

        }

        @Override
        public OperatorPlan reportChanges() {
            return subPlan;
        }

    }

}
