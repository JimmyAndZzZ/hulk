package com.jimmy.hulk.canal.base;

import com.jimmy.hulk.canal.core.CanalRowData;

import java.util.List;

public interface Callback {

    void callback(List<CanalRowData> canalRowDataList);
}
