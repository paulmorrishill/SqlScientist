import React from "react";
import {Checkbox, FormControlLabel, FormGroup, TextField} from "@material-ui/core";

const ConnectionSettings: React.FC = () => {
    return (<FormGroup>
        <div>
            <TextField required fullWidth label="Connection string" />
        </div>
        <div>
            <TextField required fullWidth label="Test data query" />
        </div>
        <div>
            <TextField required fullWidth label="Old code query" />
        </div>
        <div>
            <TextField required fullWidth label="New code query" />
        </div>
        <div>
            <FormControlLabel
                control={<Checkbox/>}
                label="Stop immediately on mismatch"
            />
        </div>
        <div>
            <TextField required fullWidth label="Maximum concurrent experiments" />
        </div>
    </FormGroup>)
};
