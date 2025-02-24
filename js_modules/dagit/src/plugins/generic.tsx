import * as React from "react";
import { startCase } from "lodash";
import { Button, Classes, Dialog } from "@blueprintjs/core";
import { IPluginSidebarProps } from ".";

export class SidebarComponent extends React.Component<IPluginSidebarProps> {
  state = {
    open: false
  };

  componentDidMount() {
    document.addEventListener("show-kind-info", this.onClick);
  }

  componentWillUnmount() {
    document.removeEventListener("show-kind-info", this.onClick);
  }

  onClick = () => {
    this.setState({
      open: true
    });
  };

  render() {
    const metadata = this.props.solid.definition.metadata;
    const spark = metadata.find(m => m.key === "kind" && m.value === "spark");
    if (!spark) {
      return <span />;
    }

    const rest = metadata
      .filter(m => m !== spark)
      .sort((a, b) => a.key.localeCompare(b.key));

    if (rest.length === 0) {
      return <span />;
    }

    return (
      <Dialog
        title={`Metadata: ${this.props.solid.name}`}
        isOpen={this.state.open}
        onClose={() =>
          this.setState({
            open: false
          })
        }
      >
        <div
          className={Classes.DIALOG_BODY}
          style={{
            maxHeight: 400,
            overflow: "scroll"
          }}
        >
          <table
            className="bp3-html-table bp3-html-table-striped"
            style={{ width: "100%" }}
          >
            <thead>
              <tr>
                <th>Key</th>
                <th>Value</th>
              </tr>
            </thead>
            <tbody>
              {rest.map(({ key, value }) => (
                <tr key={key}>
                  <td>{startCase(key)}</td>
                  <td>
                    <code style={{ whiteSpace: "pre-wrap" }}>{value}</code>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className={Classes.DIALOG_FOOTER}>
          <div className={Classes.DIALOG_FOOTER_ACTIONS}>
            <Button onClick={() => this.setState({ open: false })}>
              Close
            </Button>
          </div>
        </div>
      </Dialog>
    );
  }
}
