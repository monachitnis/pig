package org.apache.pig.newplan.logical.rules;

import java.util.Iterator;
import java.util.List;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
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
        // the pattern that this rule looks for
        // is ForEach -> UserFunc
        LogicalPlan plan = new LogicalPlan();
        LogicalRelationalOperator foreach = new LOForEach(plan);
        LogicalExpression userfunc = new UserFuncExpression(plan, null);

        plan.add(foreach);
        plan.add(userfunc);
        plan.connect(foreach, userfunc);

        return plan;
    }

    @Override
    public Transformer getNewTransformer() {
        return new NestedForEachUserFuncTransformer();
    }

    public class NestedForEachUserFuncTransformer extends Transformer {

        LOForEach foreach = null;
        LogicalExpression userfunc = null;
        LogicalRelationalOperator forEachSucc = null;
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
            LogicalPlan innerPlan = foreach.getInnerPlan();
            List<Operator> succs = innerPlan.getSuccessors(foreach);
            for (int j = 0; j < succs.size(); j++) {
                LogicalRelationalOperator logRelOp = (LogicalRelationalOperator) succs
                        .get(j);
                if (logRelOp instanceof LOInnerLoad) {
                    // infer nested foreach
                    nested = true;
                    break;
                }
            }
            if (nested) {
                List<Operator> ops = innerPlan.getSinks();
                for (Operator op : ops) {
                    if (op instanceof UserFuncExpression) {
                        return true;
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

            // DFS to userfunc
            for (Operator op1 : sinks1) {
                if (op1 instanceof UserFuncExpression) {
                    userfunc = (UserFuncExpression) op1;
                }
                if (op1.getPlan().getSinks() != null) {
                    List<Operator> sinks2 = op1.getPlan().getSinks();
                    for (Operator op2 : sinks2) {
                        if (op2 instanceof UserFuncExpression) {
                            userfunc = (UserFuncExpression) op2; ///// DO RECURSIVELY
                        }
                    }
                }
            }

            LogicalRelationalOperator foreach1 = new LOForEach(innerPlan); //how to ensure this takes tuples only?
            subPlan.add(foreach1);
            currentPlan.connect(foreach, foreach1);
            currentPlan.connect(foreach1, userfunc);

            LogicalRelationalOperator foreach2 = new LOForEach(innerPlan); //how to ensure this takes tuples only?
            subPlan.add(foreach2);
            // get the first sink from foreach --> BinCond
            Operator expOp = sinks1.get(0);
            currentPlan.connect(foreach, foreach2);
            currentPlan.connect(foreach2, expOp);

            // when subPlan when currentPlan?

        }

        @Override
        public OperatorPlan reportChanges() {
            return subPlan;
        }

    }

}
