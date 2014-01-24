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
        Set<Pair<Pair<Integer, Integer>, UserFuncExpression>> userFuncSet;
        Map<LogicalExpressionPlan, Integer> userFuncMap;
        Iterator<Pair<Pair<Integer, Integer>, UserFuncExpression>> setIter;
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

            // traverse and record userfunc instances and count
            // if (nested) {
            LOGenerate genOp = (LOGenerate) foreach.getInnerPlan().getSinks().get(0);
            List<LogicalExpressionPlan> generatePlan = genOp.getOutputPlans();
            Iterator<LogicalExpressionPlan> genIter = generatePlan.iterator();
            while (genIter.hasNext()) {
                Iterator<Operator> genOps = genIter.next().getOperators();
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
                            occur++;
                            userFuncMap.put(userFuncPlan, occur);
                            continue;
                        }
                        userFuncMap.put(userFuncPlan, 1);
                        userFuncPlan = null;
                    }
                }
            }
            return ret;
        }

        private boolean checkNestedUserFunc(LogicalExpression exp, Set visited) throws FrontendException {
            boolean top = false;
            long uid = exp.getFieldSchema().uid;
            if (exp instanceof UserFuncExpression && !visited.contains(uid)) {
                if (userFuncPlan == null) {
                    userFuncPlan = new LogicalExpressionPlan();
                    userFuncPlan.add((UserFuncExpression)exp);
                    top = true;  //top-level
                }
                List<LogicalExpression> userFuncOperands = ((UserFuncExpression) exp).getArguments();
                for (LogicalExpression ufo : userFuncOperands) {
                    if(!checkNestedUserFunc(ufo, visited)) {
                        return false;
                    }
                }
                visited.add(uid);
                if (!top && userFuncPlan != null) {
                    userFuncPlan.add(exp);
                    userFuncPlan.connect(((LogicalExpressionPlan) exp.getPlan()).getPredecessors(exp).get(0), exp);
                }
            }
            else if (exp instanceof ProjectExpression) {
                if (!projectedColumns.contains(exp.getFieldSchema().uid)) {
                    // valid column among initial projections and not from internal nested block
                    return false;
                }
                if (!top && userFuncPlan != null) {
                    ProjectExpression pex = (ProjectExpression) exp;
                    pex.setInputNum(0);
                    userFuncPlan.add(pex);
                    userFuncPlan.connect(((LogicalExpressionPlan)pex.getPlan()).getPredecessors(pex).get(0), pex);
                }
            }
            return true;
        }

        @Override
        public void transform(OperatorPlan matched) throws FrontendException {

            LogicalPlan innerPlan = foreach.getInnerPlan();
            // for splitting into two foreach stages, create a new foreach
            // operator
            LOForEach newForeach = new LOForEach(innerPlan);
            LogicalPlan newInnerPlan = new LogicalPlan();
            newForeach.setInnerPlan(newInnerPlan);
            newForeach.setAlias(foreach.getAlias());
            List<LogicalExpressionPlan> exps = new ArrayList<LogicalExpressionPlan>();
            LOGenerate newGenerate = new LOGenerate(newInnerPlan, exps, new boolean[1]); // TODO
                                                                                         // index?
            newInnerPlan.add(newGenerate);
            LOInnerLoad newInnerLoad = new LOInnerLoad(newInnerPlan, foreach, 0); // <--
            newInnerPlan.add(newInnerLoad);
            newInnerPlan.connect(newInnerLoad, newGenerate);
            System.out.println(newInnerPlan);

            // populate the project expressions under new foreach -- generate
            // iterate through userfuncset
            setIter = userFuncSet.iterator();
            while (setIter.hasNext()) {
                Pair<Pair<Integer, Integer>, UserFuncExpression> item = setIter.next();
                if (item.first.second > 1) {
                    ProjectExpression prx = new ProjectExpression(newGenerate.getPlan(), 0, item.first.first,
                            newGenerate);
                    // can be multiple projects
                    LogicalExpressionPlan lexPlan = new LogicalExpressionPlan();
                    lexPlan.add(item.second);
                    // lexPlan.add(prx);
                    lexPlan.connect(item.second, prx);
                    exps.add(lexPlan);
                }
            }

            // disconnect UserFunc from BinCond
            LOGenerate existingGen = (LOGenerate) innerPlan.getSinks().get(0);
            List<LogicalExpressionPlan> generatePlan = existingGen.getOutputPlans();
            Iterator<LogicalExpressionPlan> genIter = generatePlan.iterator();
            while (genIter.hasNext()) {
                LogicalExpressionPlan genPlan = genIter.next();
                Iterator<Operator> opIter = genPlan.getOperators();
                while (opIter.hasNext()) {
                    Operator expOp = opIter.next();
                    if (expOp instanceof UserFuncExpression) {
                        Operator column = expOp.getPlan().getSuccessors(expOp).get(0);
                        int col = (int) ((ProjectExpression) column).getFieldSchema().uid;
                        setIter = userFuncSet.iterator();
                        while (setIter.hasNext()) {
                            Pair<Pair<Integer, Integer>, UserFuncExpression> item = setIter.next();
                            if (item.second.getName() == expOp.getName() && item.first.first == col) {
                                genPlan.disconnect(genPlan.getPredecessors(expOp).get(0), expOp);
                                currentPlan.remove(expOp);
                            }
                            break;
                        }
                    }
                }
            }

            /*
             * //disconnect older unoptimized innerLoads for (Operator oldLoad :
             * innerPlan.getSources()) { innerPlan.disconnect(existingGen,
             * oldLoad); //currentPlan.remove? disconnect? }
             */

            // add new foreach as predecessor to existing foreach
            currentPlan.insertBetween(foreach, newForeach, currentPlan.getSuccessors(foreach).get(0));
            subPlan = new OperatorSubPlan(currentPlan);
            subPlan.add(newForeach);
            subPlan.add(newGenerate);
            subPlan.add(newInnerLoad);

        }

        @Override
        public OperatorPlan reportChanges() {
            return subPlan;
        }

    }

}
