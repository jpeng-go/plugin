// Copyright Fuzamei Corp. 2018 All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package executor

import (
	"github.com/33cn/chain33/types"
	pty "github.com/33cn/plugin/plugin/dapp/collateralize/types"
)

func (c *Collateralize) Query_CollateralizeInfoByID(req *pty.ReqCollateralizeInfo) (types.Message, error) {
	coll, err := queryCollateralizeByID(c.GetStateDB(), req.CollateralizeId)
	if err != nil {
		clog.Error("Query_CollateralizeInfoByID", "id", req.CollateralizeId, "error", err)
		return nil, err
	}

	info := &pty.RepCollateralizeCurrentInfo{
		Status:            coll.Status,
		TotalBalance:      coll.TotalBalance,
		DebtCeiling:       coll.DebtCeiling,
		LiquidationRatio:  coll.LiquidationRatio,
		StabilityFeeRatio: coll.StabilityFeeRatio,
		CreateAddr:        coll.CreateAddr,
		Balance:           coll.Balance,
		Period:            coll.Period,
		CollateralizeId:   coll.CollateralizeId,
		CollBalance:       coll.CollBalance,
	}
	info.BorrowRecords = append(info.BorrowRecords, coll.BorrowRecords...)
	info.BorrowRecords = append(info.BorrowRecords, coll.InvalidRecords...)

	return info, nil
}

func (c *Collateralize) Query_CollateralizeInfoByIDs(req *pty.ReqCollateralizeInfos) (types.Message, error) {
	infos := &pty.RepCollateralizeCurrentInfos{}
	for _, id := range req.CollateralizeIds {
		coll, err := queryCollateralizeByID(c.GetStateDB(), id)
		if err != nil {
			clog.Error("Query_CollateralizeInfoByID", "id", id, "error", err)
			return nil, err
		}

		info := &pty.RepCollateralizeCurrentInfo{
			Status:            coll.Status,
			TotalBalance:      coll.TotalBalance,
			DebtCeiling:       coll.DebtCeiling,
			LiquidationRatio:  coll.LiquidationRatio,
			StabilityFeeRatio: coll.StabilityFeeRatio,
			CreateAddr:        coll.CreateAddr,
			Balance:           coll.Balance,
			Period:            coll.Period,
			CollateralizeId:   coll.CollateralizeId,
			CollBalance:       coll.CollBalance,
		}
		info.BorrowRecords = append(info.BorrowRecords, coll.BorrowRecords...)
		info.BorrowRecords = append(info.BorrowRecords, coll.InvalidRecords...)

		infos.Infos = append(infos.Infos, info)
	}

	return infos, nil
}

func (c *Collateralize) Query_CollateralizeByStatus(req *pty.ReqCollateralizeByStatus) (types.Message, error) {
	ids := &pty.RepCollateralizeIDs{}
	collIDRecords, err := queryCollateralizeByStatus(c.GetLocalDB(), req.Status, req.CollID)
	if err != nil {
		clog.Error("Query_CollateralizeByStatus", "get collateralize record error", err)
		return nil, err
	}

	ids.IDs = append(ids.IDs, collIDRecords...)
	return ids, nil
}

func (c *Collateralize) Query_CollateralizeByAddr(req *pty.ReqCollateralizeByAddr) (types.Message, error) {
	ids := &pty.RepCollateralizeIDs{}
	collIDRecords, err := queryCollateralizeByAddr(c.GetLocalDB(), req.Addr, req.Status, req.CollID)
	if err != nil {
		clog.Error("Query_CollateralizeByAddr", "get collateralize record error", err)
		return nil, err
	}

	ids.IDs = append(ids.IDs, collIDRecords...)
	return ids, nil
}

func (c *Collateralize) Query_CollateralizeRecordByID(req *pty.ReqCollateralizeRecord) (types.Message, error) {
	ret := &pty.RepCollateralizeRecord{}
	issuRecord, err := queryCollateralizeRecordByID(c.GetStateDB(), req.CollateralizeId, req.RecordId)
	if err != nil {
		clog.Error("Query_IssuanceRecordByID", "get collateralize record error", err)
		return nil, err
	}

	ret.Record = issuRecord
	return ret, nil
}

func (c *Collateralize) Query_CollateralizeRecordByAddr(req *pty.ReqCollateralizeRecordByAddr) (types.Message, error) {
	ret := &pty.RepCollateralizeRecords{}
	records, err := queryCollateralizeRecordByAddr(c.GetStateDB(), c.GetLocalDB(), req.Addr, req.Status, req.CollateralizeId, req.RecordId)
	if err != nil {
		clog.Error("Query_CollateralizeRecordByAddr", "get collateralize record error", err)
		return nil, err
	}

	if req.Status == 0 {
		ret.Records = records
	} else {
		for _, record := range records {
			if record.Status == req.Status {
				ret.Records = append(ret.Records, record)
			}
		}
	}
	return ret, nil
}

func (c *Collateralize) Query_CollateralizeRecordByStatus(req *pty.ReqCollateralizeRecordByStatus) (types.Message, error) {
	ret := &pty.RepCollateralizeRecords{}
	records, err := queryCollateralizeRecordByStatus(c.GetStateDB(), c.GetLocalDB(), req.Status, req.CollateralizeId, req.RecordId)
	if err != nil {
		clog.Error("Query_CollateralizeRecordByStatus", "get collateralize record error", err)
		return nil, err
	}

	for _, record := range records {
		if record.Status == req.Status {
			ret.Records = append(ret.Records, record)
		}
	}
	return ret, nil
}

func (c *Collateralize) Query_CollateralizeConfig(req *pty.ReqCollateralizeRecordByAddr) (types.Message, error) {
	config, err := getCollateralizeConfig(c.GetStateDB())
	if err != nil {
		clog.Error("Query_CollateralizeConfig", "get collateralize config error", err)
		return nil, err
	}

	balance, err := getCollBalance(config.TotalBalance, c.GetLocalDB(), c.GetStateDB())
	if err != nil {
		clog.Error("Query_CollateralizeInfoByID", "error", err)
		return nil, err
	}

	ret := &pty.RepCollateralizeConfig{
		TotalBalance:      config.TotalBalance,
		DebtCeiling:       config.DebtCeiling,
		LiquidationRatio:  config.LiquidationRatio,
		StabilityFeeRatio: config.StabilityFeeRatio,
		Period:            config.Period,
		Balance:           balance,
		CurrentTime:       config.CurrentTime,
	}

	return ret, nil
}

func (c *Collateralize) Query_CollateralizePrice(req *pty.ReqCollateralizeRecordByAddr) (types.Message, error) {
	price, err := getLatestPrice(c.GetStateDB())
	if err != nil {
		clog.Error("Query_CollateralizePrice", "error", err)
		return nil, err
	}

	return &pty.RepCollateralizePrice{Price: price}, nil
}

// 查询用户借贷金额
func (c *Collateralize) Query_CollateralizeUserBalance(req *pty.ReqCollateralizeRecordByAddr) (types.Message, error) {
	balance, err := queryCollateralizeUserBalance(c.GetStateDB(), c.GetLocalDB(), req.Addr)
	if err != nil {
		clog.Error("Query_CollateralizeRecordByAddr", "get collateralize record error", err)
		return nil, err
	}

	return &pty.RepCollateralizeUserBalance{Balance: balance}, nil
}

// 查询放贷总状态
func (c *Collateralize) Query_CollateralizeLendStatus(req *pty.ReqCollateralizeRecordByAddr) (types.Message, error) {
	var collIDs []string
	var collIDRecords []string
	var primary string

	for {
		collIDRecords, _ = queryCollateralizeByStatus(c.GetLocalDB(), pty.CollateralizeStatusCreated, primary)
		collIDs = append(collIDs, collIDRecords...)

		if len(collIDRecords) < int(DefaultCount) {
			break
		}
		primary = collIDRecords[DefaultCount-1]
	}

	rep := &pty.RepCollateralizeLendStatus{}
	rep.ConfigBalance = getCollerConfig(c.GetStateDB())
	for _, collID := range collIDs {
		collInfo, err := queryCollateralizeByID(c.GetStateDB(), collID)
		if err != nil {
			clog.Error("Query_CollateralizeInfoByID", "id", collID, "error", err)
			return nil, err
		}
		rep.RecordNum += int64(len(collInfo.BorrowRecords))
		rep.TotalCollBalance += collInfo.CollBalance
		rep.TotalLendBalance += (collInfo.TotalBalance - collInfo.Balance)
	}

	return rep, nil
}

// 查询大户已放出和待放出金额
func (c *Collateralize) Query_CollateralizeLenderBalance(req *pty.ReqCollateralizeByAddr) (types.Message, error) {
	var collIDs []string
	var collIDRecords []string
	var primary string

	for {
		collIDRecords, _ = queryCollateralizeByAddr(c.GetLocalDB(), req.Addr, pty.CollateralizeStatusCreated, primary)
		collIDs = append(collIDs, collIDRecords...)

		if len(collIDRecords) < int(DefaultCount) {
			break
		}
		primary = collIDRecords[DefaultCount-1]
	}

	rep := &pty.RepCollateralizeLenderBalance{}
	for _, collID := range collIDs {
		collInfo, err := queryCollateralizeByID(c.GetStateDB(), collID)
		if err != nil {
			clog.Error("Query_CollateralizeInfoByID", "id", collID, "error", err)
			return nil, err
		}
		rep.TotalLendingBalance += collInfo.Balance
		rep.TotalLentBalance += (collInfo.TotalBalance - collInfo.Balance)
	}

	return rep, nil
}
