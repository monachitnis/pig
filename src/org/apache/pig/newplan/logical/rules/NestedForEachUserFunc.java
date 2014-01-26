package org.apache.pig.newplan.logical.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
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
        Map<LogicalExpressionPlan, Integer> userFuncMap;
        Iterator<Entry<LogicalExpressionPlan, Integer>> mapIter;
        Set<Long> projectedColumns;
        OperatorSubPlan subPlan = null;
        LogicalExpressionPlan userFuncPlan = null;

        @Override
        public boolean check(OperatorPlan matched) throws FrontendException {
            boolean ret = false;
            Iterator<Operator> iter = matched.getOperators();
            while (iter.hasNext()) {
                Operator op = iter.next();
                if (op instanceof LOForEach) {
                    foreach = (LOForEach) op;
                    break;
                }
            }

            List<Operator> sources = foreach.getPlan().getSources();
            for (Operator source : sources) {
                LOLoad load = (LOLoad) source;
                LogicalSchema schema = load.getSchema();
                if (schema != null) {
                    if (projectedColumns == null) {
                        projectedColumns = new HashSet<Long>();
                    }
                    List<LogicalFieldSchema> fields = schema.getFields();
                    for (LogicalFieldSchema field : fields) {
                        projectedColumns.add(field.uid);
                    }
                }
            }

            // traverse and record userfunc plans and count
            LOGenerate gen = (LOGenerate) foreach.getInnerPlan().getSinks().get(0);
            for (LogicalExpressionPlan genPlan : gen.getOutputPlans()) {
                Iterator<Operator> genOps = genPlan.getOperators();
                Set<Long> visited = new HashSet<Long>();
                while (genOps.hasNext()) {
                    Operator expOp = genOps.next();
                    // check for nested userfunc and its operands
                    if (checkNestedUserFunc((LogicalExpression) expOp, visited) && userFuncPlan != null) {
                        if (userFuncMap == null) {
                            userFuncMap = new HashMap<LogicalExpressionPlan, Integer>();
                        }
                        Integer occur = userFuncMap.get(userFuncPlan);
                        if (occur != null) {
                            userFuncMap.put(userFuncPlan, ++occur);
                            ret = true;
                        }
                        else {
                            userFuncMap.put(userFuncPlan, 1);
                        }
                        userFuncPlan = null;
                    }
                }
            }
            return ret;
        }

        // recursive method
        private boolean checkNestedUserFunc(LogicalExpression exp, Set<Long> visited) throws FrontendException {
            boolean top = false;
            long uid = exp.getFieldSchema().uid;
            if (exp instanceof ProjectExpression) {
                if (!projectedColumns.contains(exp.getFieldSchema().uid)) {
                    // valid column among initial projections and not from internal nested block
                    return false;
                }
                if (!top && userFuncPlan != null) {
                    ProjectExpression pex = (ProjectExpression) exp;
                    //pex.setInputNum(0); //for equal comparison
                    //pex.setStartAlias(exp.toName()); // alias not appearing
                    userFuncPlan.add(pex);
                    userFuncPlan.connect(((LogicalExpressionPlan)pex.getPlan()).getPredecessors(pex).get(0), pex);
                }
            }
            else {
                if (exp instanceof UserFuncExpression && !visited.contains(uid)) {
                    if (userFuncPlan == null) {
                        userFuncPlan = new LogicalExpressionPlan();
                        userFuncPlan.add((UserFuncExpression) exp);
                        top = true; // top-level
                    }
                    List<LogicalExpression> userFuncOperands = ((UserFuncExpression) exp).getArguments();
                    for (LogicalExpression ufo : userFuncOperands) {
                        if (!checkNestedUserFunc(ufo, visited)) {
                            return false;
                        }
                    }
                    visited.add(uid);

                }
                if (!top && userFuncPlan != null) {
                    userFuncPlan.add(exp);
                    userFuncPlan.connect(((LogicalExpressionPlan) exp.getPlan()).getPredecessors(exp).get(0), exp);
                }
            }
            return true;
        }

        @Override
        public void transform(OperatorPlan matched) throws FrontendException {

            LogicalPlan innerPlan = foreach.getInnerPlan();
            // for splitting into two foreach stages, create a new foreach operator
            LOForEach newForeach = new LOForEach(innerPlan);
            LogicalPlan newInnerPlan = new LogicalPlan();
            newForeach.setInnerPlan(newInnerPlan);
            newForeach.setAlias(foreach.getAlias());
            List<LogicalExpressionPlan> exps = new ArrayList<LogicalExpressionPlan>();
            LOGenerate newGenerate = new LOGenerate(newInnerPlan, exps, new boolean[1]);
            newInnerPlan.add(newGenerate);

            subPlan = new OperatorSubPlan(currentPlan);
            subPlan.add(newForeach);
            subPlan.add(newGenerate);
            currentPlan.connect(newForeach, newGenerate);
            System.out.println(currentPlan);

            // populate the project expressions under new foreach -- generate
            // iterate through userfuncset
            mapIter = userFuncMap.entrySet().iterator();
            while (mapIter.hasNext()) {
                Entry<LogicalExpressionPlan, Integer> e = mapIter.next();
                if (e.getValue() > 1) {
                    LogicalExpressionPlan toMove = e.getKey();
                    exps.add(toMove);
                    LOInnerLoad newInnerLoad = new LOInnerLoad(toMove, foreach, 0);
                    newInnerPlan.add(newInnerLoad);
                    newInnerPlan.connect(newInnerLoad, newGenerate);

                    Operator userFunc = toMove.getOperators().next();

                    // disconnect duplicate plans
                    LOGenerate gen = (LOGenerate) innerPlan.getSinks().get(0);
                    for (LogicalExpressionPlan genPlan : gen.getOutputPlans()) {
                        Iterator<Operator> genOps = genPlan.getOperators();
                        while (genOps.hasNext()) {
                            Operator genOp = genOps.next();
                            Operator parent = genOp.getPlan().getPredecessors(userFunc).get(0);
                            if (genOp instanceof UserFuncExpression
                                    && toMove.getSinks().get(0) == parent.getPlan().getSinks().get(0)) {
                                genPlan.disconnect(parent, genOp);
                                Iterator<Operator> rem = genOp.getPlan().getOperators();
                                rem.next(); // skip the genOp itself
                                while (rem.hasNext()) {
                                    Operator toRem = rem.next();
                                    genPlan.disconnect(genOp, toRem);
                                    genPlan.remove(toRem); //TODO throws 'still connected' error
                                }
                                genPlan.remove(genOp);
                            }
                        }
                    }
                }
            }

            // Adjust attachedOp
            for (LogicalExpressionPlan p : newGenerate.getOutputPlans()) {
                Iterator<Operator> iter = p.getOperators();
                while (iter.hasNext()) {
                    Operator op = iter.next();
                    if (op instanceof ProjectExpression) {
                        ((ProjectExpression)op).setAttachedRelationalOp(newGenerate);
                    }
                }
            }

            Iterator<Operator> iter = newForeach.getInnerPlan().getOperators();
            while (iter.hasNext()) {
                Operator op = iter.next();
                if (op instanceof LOInnerLoad) {
                    ((LOInnerLoad)op).getProjection().setAttachedRelationalOp(newForeach);
                }
            }

            // add new foreach as predecessor to existing foreach
            currentPlan.insertBetween(foreach, newForeach, currentPlan.getSuccessors(foreach).get(0));
            //TODO checking new foreach supplies proper input num to successor BinCond

        }

        @Override
        public OperatorPlan reportChanges() {
            return subPlan;
        }

    }

}
