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
            LogicalExpressionPlan genPlanOrig = null;
            int indexOrig = 0;
            List<LogicalExpressionPlan> genPlans = gen.getOutputPlans();
            for (int index = 0 ; index < genPlans.size() ; index++) {
                Iterator<Operator> genOps = genPlans.get(index).getOperators();
                Set<Long> visited = new HashSet<Long>();
                while (genOps.hasNext()) {
                    Operator genOp = genOps.next();
                    // check for nested userfunc and its operands
                    if (checkNestedUserFunc((LogicalExpression) genOp, visited) && userFuncPlan != null) {
                        if (userFuncMap == null) {
                            userFuncMap = new HashMap<LogicalExpressionPlan, Integer>();
                        }
                        int occurrence = 0;
                        mapIter = userFuncMap.entrySet().iterator();
                        while (mapIter.hasNext()) {
                            Entry<LogicalExpressionPlan, Integer> duplicate = mapIter.next();
                            if (isLogicallyEqual(duplicate.getKey(), userFuncPlan)) {
                                UserFuncExpression userFunc = (UserFuncExpression) duplicate.getKey().getOperators().next();
                                ProjectExpression col = (ProjectExpression) userFunc.getPlan().getSinks().get(0); //TODO multiple sinks i.e. operands

                                occurrence = duplicate.getValue();
                                if (occurrence == 1) { //first time match for this userFuncPlan
                                    Operator parent = genPlanOrig.getPredecessors(userFunc).get(0);
                                    disconnectPlan(genPlanOrig, duplicate.getKey());

                                    // add new project
                                    ProjectExpression pe = new ProjectExpression(genPlanOrig, indexOrig,
                                            col.getColAlias(), null, gen);
                                    pe.setColumnNumberFromAlias();
                                    genPlanOrig.connect(parent, pe);
                                }
                                duplicate.setValue(++occurrence);
                                ret = true;

                                UserFuncExpression userFuncOp = (UserFuncExpression) userFuncPlan.getOperators().next();
                                Operator parent = genPlans.get(index).getPredecessors(userFuncOp).get(0);
                                col = (ProjectExpression) userFuncPlan.getSinks().get(0); //TODO multiple sinks i.e. operands

                                disconnectPlan(genPlans.get(index), userFuncPlan);

                                // add new project
                                ProjectExpression pe = new ProjectExpression(genPlans.get(index), index,
                                        col.getColAlias(), null, gen);
                                genPlans.get(index).connect(parent, pe);
                            }
                        }
                        if (occurrence == 0) {
                            userFuncMap.put(userFuncPlan, 1);
                            genPlanOrig = genPlans.get(index);
                            indexOrig = index;
                        }
                        userFuncPlan = null;
                    }
                    break;
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

        // disconnect userFuncPlan from original Generate
        private void disconnectPlan(OperatorPlan outerPlan, LogicalExpressionPlan toDisc) throws FrontendException {
            Operator userFuncOp = toDisc.getOperators().next();
            Operator parent = outerPlan.getPredecessors(userFuncOp).get(0);
            outerPlan.disconnect(parent, userFuncOp);
            Iterator<Operator> rem = toDisc.getOperators();
            rem.next(); // skip the userfunc itself
            while (rem.hasNext()) {
                Operator toRem = rem.next();
                outerPlan.disconnect(outerPlan.getPredecessors(toRem).get(0), toRem);
                outerPlan.remove(toRem);
            }
            outerPlan.remove(userFuncOp);
        }

        private boolean isLogicallyEqual(LogicalExpressionPlan l1, LogicalExpressionPlan l2) throws FrontendException {
            List<Operator> roots = l1.getSources();
            List<Operator> otherRoots = l2.getSources();
            if (roots.size() == 0 && otherRoots.size() == 0)
                return true;
            if (roots.size() > 1 || otherRoots.size() > 1) {
                throw new FrontendException("Found LogicalExpressionPlan with more than one root.  Unexpected.", 2224);
            }
            return isLogicallyEqual(roots.get(0), otherRoots.get(0));
        }

        private boolean isLogicallyEqual(Operator o1, Operator o2) throws FrontendException {
            if (o1 instanceof UserFuncExpression && o2 instanceof UserFuncExpression) {
                UserFuncExpression u1 = (UserFuncExpression) o1;
                UserFuncExpression u2 = (UserFuncExpression) o2;
                if (!u1.getFuncSpec().equals(u2.getFuncSpec()))
                    return false;

                List<Operator> mySuccs = u1.getPlan().getSuccessors(u1);
                List<Operator> theirSuccs = u2.getPlan().getSuccessors(u2);
                if (mySuccs == null || theirSuccs == null) {
                    if (mySuccs == null && theirSuccs == null) {
                        return true;
                    }
                    else {
                        // only one of the udfs has null successors
                        return false;
                    }
                }
                if (mySuccs.size() != theirSuccs.size())
                    return false;
                for (int i = 0; i < mySuccs.size(); i++) {
                    if (mySuccs.get(i) instanceof ProjectExpression || mySuccs.get(i) instanceof UserFuncExpression) {
                        if (!isLogicallyEqual(mySuccs.get(i), theirSuccs.get(i)))
                            return false;
                    }
                    else {
                        if (!mySuccs.get(i).isEqual(theirSuccs.get(i)))
                            return false;
                    }
                }
                return true;
            }
            else {
                if (o1 instanceof ProjectExpression && o2 instanceof ProjectExpression) {
                    Operator mySucc = o1.getPlan().getSuccessors(o1) != null
                            ? o1.getPlan().getSuccessors(o1).get(0)
                            : null;
                    Operator theirSucc = o2.getPlan().getSuccessors(o2) != null
                            ? o2.getPlan().getSuccessors(o2).get(0)
                            : null;
                    if (mySucc != null && theirSucc != null)
                        return mySucc.isEqual(theirSucc);
                    if (mySucc == null && theirSucc == null)
                        return true;
                    return false;
                }
            }
            return false;
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
            LOInnerLoad newInnerLoad = new LOInnerLoad(newInnerPlan, newForeach, 0);
            newInnerLoad.setSchema(((LOLoad)foreach.getPlan().getSources().get(0)).getSchema());
            newInnerPlan.add(newInnerLoad);
            newInnerPlan.connect(newInnerLoad, newGenerate);

            subPlan = new OperatorSubPlan(currentPlan);
            subPlan.add(foreach);
            subPlan.add(newForeach);
            subPlan.add(newGenerate);
            subPlan.add(newInnerLoad);
            currentPlan.connect(newForeach, newGenerate);

            // add new foreach as predecessor to existing foreach
            subPlan.add(currentPlan.getPredecessors(foreach).get(0));
            currentPlan.insertBetween(currentPlan.getPredecessors(foreach).get(0), newForeach, foreach);

            LOGenerate gen = (LOGenerate) innerPlan.getSinks().get(0);
            // populate the project expressions under new foreach -- generate
            // iterate through userfuncset
            mapIter = userFuncMap.entrySet().iterator();
            while (mapIter.hasNext()) {
                Entry<LogicalExpressionPlan, Integer> e = mapIter.next();
                if (e.getValue() > 1) {
                    LogicalExpressionPlan toMove = e.getKey();
                    exps.add(toMove);

                    // change innerLoad of generate to load this new column
                    // this affects project indirectly
                    for (Operator genSrc : gen.getPlan().getSources()) {
                        LOInnerLoad innerLoad = (LOInnerLoad) genSrc;
                        innerLoad.setSchema(newForeach.getSchema());
                    }
                }
            }

            for (OperatorPlan genPlan : gen.getOutputPlans()) {
                Iterator<Operator> genOps = genPlan.getOperators();
                while (genOps.hasNext()) {
                    Operator genOp = genOps.next();
                    if (genOp instanceof ProjectExpression) {
                        ProjectExpression projOp = (ProjectExpression) genOp;
                        projOp.getFieldSchema();
                        projOp.setAttachedRelationalOp(gen);
                    }
                }
            }
        }

        @Override
        public OperatorPlan reportChanges() {
            return subPlan;
        }

    }

}
