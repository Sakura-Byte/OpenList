package openlist

import (
	"context"
	"fmt"
	"net/http"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/go-resty/resty/v2"
	log "github.com/sirupsen/logrus"
)

func (d *OpenList) login(ctx context.Context) error {
	if d.Username == "" {
		return nil
	}
	var resp common.Resp[LoginResp]
	_, _, err := d.request(ctx, "/auth/login", http.MethodPost, func(req *resty.Request) {
		req.SetResult(&resp).SetBody(base.Json{
			"username": d.Username,
			"password": d.Password,
		})
	})
	if err != nil {
		return err
	}
	d.Token = resp.Data.Token
	op.MustSaveDriverStorage(d)
	return nil
}

func (d *OpenList) request(ctx context.Context, api, method string, callback base.ReqCallback, retry ...bool) ([]byte, int, error) {
	url := d.Address + "/api" + api
	if err := d.WaitLimit(ctx); err != nil {
		return nil, 0, err
	}
	req := base.RestyClient.R().SetContext(ctx)
	req.SetHeader("Authorization", d.Token)
	if callback != nil {
		callback(req)
	}
	res, err := req.Execute(method, url)
	if err != nil {
		code := 0
		if res != nil {
			code = res.StatusCode()
		}
		return nil, code, err
	}
	log.Debugf("[openlist] response body: %s", res.String())
	if res.StatusCode() >= 400 {
		return nil, res.StatusCode(), fmt.Errorf("request failed, status: %s", res.Status())
	}
	code := utils.Json.Get(res.Body(), "code").ToInt()
	if code != 200 {
		if (code == 401 || code == 403) && !utils.IsBool(retry...) {
			err = d.login(ctx)
			if err != nil {
				return nil, code, err
			}
			return d.request(ctx, api, method, callback, true)
		}
		return nil, code, fmt.Errorf("request failed,code: %d, message: %s", code, utils.Json.Get(res.Body(), "message").ToString())
	}
	return res.Body(), 200, nil
}
