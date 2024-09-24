package crvorm

import (
	
)

type CrvOrm struct {
	Repo DataRepository
}

func (orm *CrvOrm)InitDefaultRepo(dbConf *DbConf)(error){
	repo:=&DefatultDataRepository{}
	err:=repo.Connect(dbConf)
	if err!=nil{
		return err
	}
	orm.Repo=repo
	return nil
}

func (orm *CrvOrm)ExecuteQuery(queryParam *QueryParam)(*QueryResult,error){
	return ExecuteQuery(queryParam,orm.Repo,true)
}