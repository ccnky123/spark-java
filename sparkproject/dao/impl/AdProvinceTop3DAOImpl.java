package com.ibeifeng.sparkproject.dao.impl;

import java.util.ArrayList;
import java.util.List;

import com.ibeifeng.sparkproject.dao.IAdProvinceTop3DAO;
import com.ibeifeng.sparkproject.domain.AdProvinceTop3;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

/**
 * top3 advertisements amoung all province implementation class
 * @author Administrator
 *
 */
public class AdProvinceTop3DAOImpl implements IAdProvinceTop3DAO {

	@Override
	public void updateBatch(List<AdProvinceTop3> adProvinceTop3s) {
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		
		// distinct（date_province）
		List<String> dateProvinces = new ArrayList<String>();
		
		for(AdProvinceTop3 adProvinceTop3 : adProvinceTop3s) {
			String date = adProvinceTop3.getDate();
			String province = adProvinceTop3.getProvince();
			String key = date + "_" + province;
			
			if(!dateProvinces.contains(key)) {
				dateProvinces.add(key);
			}
		}
		
		// distinct date and province,delete batch
		String deleteSQL = "DELETE FROM ad_province_top3 WHERE date=? AND province=?";
		
		List<Object[]> deleteParamsList = new ArrayList<Object[]>();
		
		for(String dateProvince : dateProvinces) {
			String[] dateProvinceSplited = dateProvince.split("_");
			String date = dateProvinceSplited[0];
			String province = dateProvinceSplited[1];
			
			Object[] params = new Object[]{date, province};
			deleteParamsList.add(params);
		}
		
		jdbcHelper.executeBatch(deleteSQL, deleteParamsList);
		
		// insert data
		String insertSQL = "INSERT INTO ad_province_top3 VALUES(?,?,?,?)";  
		
		List<Object[]> insertParamsList = new ArrayList<Object[]>();
		
		for(AdProvinceTop3 adProvinceTop3 : adProvinceTop3s) {
			Object[] params = new Object[]{adProvinceTop3.getDate(),
					adProvinceTop3.getProvince(),
					adProvinceTop3.getAdid(),
					adProvinceTop3.getClickCount()};
			
			insertParamsList.add(params);
		}
		
		jdbcHelper.executeBatch(insertSQL, insertParamsList);
	}

}
