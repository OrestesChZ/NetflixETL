# Databricks notebook source
var = dbutils.jobs.taskValues.get(taskKey = "WeekdayLookUp", key = "weekoutput", debugValue="default_value")