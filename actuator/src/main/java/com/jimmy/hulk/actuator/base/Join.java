package com.jimmy.hulk.actuator.base;

import com.jimmy.hulk.actuator.core.Fragment;
import com.jimmy.hulk.actuator.core.Row;
import com.jimmy.hulk.parse.core.element.ConditionGroupNode;
import com.jimmy.hulk.parse.core.element.TableNode;

import java.util.List;

public interface Join {

    List<Row> join(List<Row> master, List<Fragment> slave, TableNode tableNode, List<ConditionGroupNode> conditionGroupNodes);

}
